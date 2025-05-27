import pandas as pd
import numpy as np
import json
from typing import List, Dict, Optional

from ..utils import (
    get_config,
    get_file_path,
    validate_file_exists,
    safe_read_csv,
    safe_write_csv,
    log_step,
    log_info,
    log_warning,
    log_error
)


def add_ownership_data(
    input_file: str = "companies_with_industry.csv",
    output_file: str = "companies_with_ownership.csv",
    shareholders_file: str = "shareholders.json",
    top_percentages: int = 3,
    top_names: int = 3,
    filters: dict = None
) -> pd.DataFrame:
    log_step("ADDING OWNERSHIP DATA")
    log_info(f"Loading companies from {input_file}")

    companies_df = safe_read_csv(input_file)
    if companies_df is None:
        log_error(f"Failed to load input file {input_file}")
        return None

    log_info(f"Loaded {len(companies_df)} companies")

    shareholders_path = get_file_path(shareholders_file)
    if not validate_file_exists(shareholders_file):
        log_error(f"Shareholders file {shareholders_file} not found")
        return companies_df

    log_info(f"Loading shareholders data from {shareholders_file}")

    try:
        with open(shareholders_path, 'r', encoding='utf-8') as f:
            shareholders_data = json.load(f)

        log_info(f"Loaded data for {len(shareholders_data)} companies from shareholders file")

        shareholders_dict = {str(company['ariregistri_kood']): company for company in shareholders_data}

        companies_df['owner_count'] = 0
        companies_df[f'top_{top_percentages}_percentages'] = None
        companies_df[f'top_{top_names}_owners'] = None

        processed_count = 0
        matched_count = 0

        for idx, row in companies_df.iterrows():
            company_code = str(row['company_code'])

            if company_code in shareholders_dict:
                matched_count += 1
                company_data = shareholders_dict[company_code]
                shareholders = company_data.get('osanikud', [])

                owner_count = len(shareholders)
                companies_df.at[idx, 'owner_count'] = owner_count

                if owner_count > 0:
                    sorted_shareholders = sorted(
                        shareholders,
                        key=lambda x: float(x.get('osaluse_protsent', '0') or '0'),
                        reverse=True
                    )

                    top_perc = [float(s.get('osaluse_protsent', '0') or '0') for s in sorted_shareholders[:top_percentages]]
                    top_perc.extend([0] * (top_percentages - len(top_perc)))
                    companies_df.at[idx, f'top_{top_percentages}_percentages'] = str(top_perc)

                    top_owner_names = []
                    for s in sorted_shareholders[:top_names]:
                        first_name = s.get('eesnimi', '')
                        last_name = s.get('nimi_arinimi', '')
                        if first_name and last_name:
                            full_name = f"{first_name} {last_name}"
                        else:
                            full_name = last_name or first_name or 'Unknown'
                        top_owner_names.append(full_name)

                    top_owner_names.extend([''] * (top_names - len(top_owner_names)))
                    companies_df.at[idx, f'top_{top_names}_owners'] = str(top_owner_names)

            processed_count += 1
            if processed_count % 10000 == 0:
                log_info(f"Processed {processed_count} companies")

        log_info(f"Found ownership data for {matched_count} out of {len(companies_df)} companies")

        if filters:
            original_count = len(companies_df)

            if 'owner_count' in filters:
                count_filter = filters['owner_count']

                if 'exact' in count_filter and count_filter['exact']:
                    exact_values = count_filter['exact']
                    if not isinstance(exact_values, list):
                        exact_values = [exact_values]
                    companies_df = companies_df[companies_df['owner_count'].isin(exact_values)]
                    log_info(f"Filtered to companies with exactly {exact_values} owners: {len(companies_df)} remaining")

                elif 'min' in count_filter or 'max' in count_filter:
                    if 'min' in count_filter and count_filter['min'] is not None:
                        companies_df = companies_df[companies_df['owner_count'] >= count_filter['min']]
                        log_info(f"Filtered to companies with at least {count_filter['min']} owners: {len(companies_df)} remaining")

                    if 'max' in count_filter and count_filter['max'] is not None:
                        companies_df = companies_df[companies_df['owner_count'] <= count_filter['max']]
                        log_info(f"Filtered to companies with at most {count_filter['max']} owners: {len(companies_df)} remaining")

            if 'percentages' in filters and len(companies_df) > 0:
                percentage_filter = filters['percentages']

                def check_percentages(percentages_str, filter_config):
                    if not percentages_str or percentages_str == 'None':
                        return False

                    try:
                        percentages = eval(percentages_str)

                        if 'exact' in filter_config and filter_config['exact']:
                            exact_values = filter_config['exact']
                            if not isinstance(exact_values[0], list):
                                exact_values = [exact_values]

                            for exact_pattern in exact_values:
                                if len(exact_pattern) <= len(percentages):
                                    match = True
                                    for i, target in enumerate(exact_pattern):
                                        if abs(percentages[i] - target) > 0.1:
                                            match = False
                                            break
                                    if match:
                                        return True
                            return False

                        if 'min' in filter_config and filter_config['min'] is not None:
                            min_val = filter_config['min']
                            if min_val is not None and percentages[0] < min_val:
                                return False

                        if 'max' in filter_config and filter_config['max'] is not None:
                            max_val = filter_config['max']
                            if max_val is not None and percentages[0] > max_val:
                                return False

                        return True

                    except:
                        return False

                perc_col = f'top_{top_percentages}_percentages'
                mask = companies_df[perc_col].apply(lambda x: check_percentages(x, percentage_filter))
                filtered_df = companies_df[mask]

                if 'exact' in percentage_filter and percentage_filter['exact']:
                    exact_values = percentage_filter['exact']
                    log_info(f"Filtered to companies with ownership percentages matching {exact_values}: {len(filtered_df)} remaining")
                else:
                    log_info(f"Filtered by ownership percentage range: {len(filtered_df)} remaining")

                companies_df = filtered_df

            filtered_out = original_count - len(companies_df)
            log_info(f"Ownership filters removed {filtered_out} companies")

        for idx, row in companies_df.iterrows():
            if row[f'top_{top_percentages}_percentages'] and row[f'top_{top_percentages}_percentages'] != 'None':
                try:
                    percentages = eval(row[f'top_{top_percentages}_percentages'])
                    formatted = [f"{p:.2f}%" for p in percentages if p > 0]
                    companies_df.at[idx, f'top_{top_percentages}_percentages'] = ', '.join(formatted)
                except:
                    pass

            if row[f'top_{top_names}_owners'] and row[f'top_{top_names}_owners'] != 'None':
                try:
                    owners = eval(row[f'top_{top_names}_owners'])
                    companies_df.at[idx, f'top_{top_names}_owners'] = ', '.join([o for o in owners if o])
                except:
                    pass

        if safe_write_csv(companies_df, output_file):
            log_info(f"Saved {len(companies_df)} companies with ownership data to {output_file}")
        else:
            log_error(f"Failed to save results to {output_file}")

        return companies_df

    except Exception as e:
        log_error(f"Error processing ownership data: {str(e)}")
        import traceback
        traceback.print_exc()
        return companies_df
