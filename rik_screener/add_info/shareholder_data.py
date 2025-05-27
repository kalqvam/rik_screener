import pandas as pd
import os
import numpy as np
import json
from typing import List, Dict, Optional

# Configuration
BASE_PATH = "/content/drive/MyDrive/Python/rik_screener"

def add_ownership_data(
    input_file: str = "companies_with_industry.csv",
    output_file: str = "companies_with_ownership.csv",
    shareholders_file: str = "shareholders.json",
    top_percentages: int = 3,  # Show top X ownership percentages
    top_names: int = 3,        # Show top Y owner names
    filters: dict = None       # Ownership filters
) -> pd.DataFrame:
    print(f"\n=== STEP 2.6: ADDING OWNERSHIP DATA ===")
    print(f"Loading companies from {input_file}...")

    input_path = os.path.join(BASE_PATH, input_file)
    if not os.path.exists(input_path):
        print(f"Error: Input file {input_path} not found")
        return None

    companies_df = pd.read_csv(input_path)
    print(f"Loaded {len(companies_df)} companies")

    # Load the shareholders data
    shareholders_path = os.path.join(BASE_PATH, shareholders_file)
    if not os.path.exists(shareholders_path):
        print(f"Error: Shareholders file {shareholders_path} not found")
        return companies_df  # Return original data without ownership data

    print(f"Loading shareholders data from {shareholders_file}...")

    try:
        import json
        with open(shareholders_path, 'r', encoding='utf-8') as f:
            shareholders_data = json.load(f)

        print(f"Loaded data for {len(shareholders_data)} companies from shareholders file")

        # Create a lookup dictionary for faster access
        shareholders_dict = {str(company['ariregistri_kood']): company for company in shareholders_data}

        # Create new columns for ownership data
        companies_df['owner_count'] = 0
        companies_df[f'top_{top_percentages}_percentages'] = None
        companies_df[f'top_{top_names}_owners'] = None

        # Process each company
        processed_count = 0
        matched_count = 0

        for idx, row in companies_df.iterrows():
            company_code = str(row['company_code'])

            if company_code in shareholders_dict:
                matched_count += 1
                company_data = shareholders_dict[company_code]
                shareholders = company_data.get('osanikud', [])

                # Count owners
                owner_count = len(shareholders)
                companies_df.at[idx, 'owner_count'] = owner_count

                if owner_count > 0:
                    # Sort shareholders by percentage (descending)
                    sorted_shareholders = sorted(
                        shareholders,
                        key=lambda x: float(x.get('osaluse_protsent', '0') or '0'),
                        reverse=True
                    )

                    # Get top percentages
                    top_perc = [float(s.get('osaluse_protsent', '0') or '0') for s in sorted_shareholders[:top_percentages]]
                    # Pad with zeros if needed
                    top_perc.extend([0] * (top_percentages - len(top_perc)))
                    companies_df.at[idx, f'top_{top_percentages}_percentages'] = str(top_perc)

                    # Get top owner names
                    top_owner_names = []
                    for s in sorted_shareholders[:top_names]:
                        first_name = s.get('eesnimi', '')
                        last_name = s.get('nimi_arinimi', '')
                        if first_name and last_name:
                            full_name = f"{first_name} {last_name}"
                        else:
                            full_name = last_name or first_name or 'Unknown'
                        top_owner_names.append(full_name)

                    # Pad with empty strings if needed
                    top_owner_names.extend([''] * (top_names - len(top_owner_names)))
                    companies_df.at[idx, f'top_{top_names}_owners'] = str(top_owner_names)

            processed_count += 1
            if processed_count % 10000 == 0:
                print(f"Processed {processed_count} companies...")

        print(f"Found ownership data for {matched_count} out of {len(companies_df)} companies")

        # Apply ownership filters if specified
        if filters:
            original_count = len(companies_df)

            # Filter by owner count
            if 'owner_count' in filters:
                count_filter = filters['owner_count']

                # Exact match filter
                if 'exact' in count_filter and count_filter['exact']:
                    exact_values = count_filter['exact']
                    if not isinstance(exact_values, list):
                        exact_values = [exact_values]
                    companies_df = companies_df[companies_df['owner_count'].isin(exact_values)]
                    print(f"Filtered to companies with exactly {exact_values} owners: {len(companies_df)} remaining")

                # Min/max filters
                elif 'min' in count_filter or 'max' in count_filter:
                    if 'min' in count_filter and count_filter['min'] is not None:
                        companies_df = companies_df[companies_df['owner_count'] >= count_filter['min']]
                        print(f"Filtered to companies with at least {count_filter['min']} owners: {len(companies_df)} remaining")

                    if 'max' in count_filter and count_filter['max'] is not None:
                        companies_df = companies_df[companies_df['owner_count'] <= count_filter['max']]
                        print(f"Filtered to companies with at most {count_filter['max']} owners: {len(companies_df)} remaining")

            # Filter by ownership percentages
            if 'percentages' in filters and len(companies_df) > 0:
                percentage_filter = filters['percentages']

                # This is more complex as the percentages are stored as strings of lists
                # We'll need to create a mask for filtering

                # Helper function to check if percentages match the filter
                def check_percentages(percentages_str, filter_config):
                    if not percentages_str or percentages_str == 'None':
                        return False

                    try:
                        # Convert string representation to actual list
                        percentages = eval(percentages_str)

                        # Check exact match
                        if 'exact' in filter_config and filter_config['exact']:
                            exact_values = filter_config['exact']
                            if not isinstance(exact_values[0], list):
                                exact_values = [exact_values]

                            for exact_pattern in exact_values:
                                if len(exact_pattern) <= len(percentages):
                                    match = True
                                    for i, target in enumerate(exact_pattern):
                                        # Allow small tolerance (0.1%) for float comparison
                                        if abs(percentages[i] - target) > 0.1:
                                            match = False
                                            break
                                    if match:
                                        return True
                            return False

                        # Check min/max
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

                # Apply the filter
                perc_col = f'top_{top_percentages}_percentages'
                mask = companies_df[perc_col].apply(lambda x: check_percentages(x, percentage_filter))
                filtered_df = companies_df[mask]

                if 'exact' in percentage_filter and percentage_filter['exact']:
                    exact_values = percentage_filter['exact']
                    print(f"Filtered to companies with ownership percentages matching {exact_values}: {len(filtered_df)} remaining")
                else:
                    print(f"Filtered by ownership percentage range: {len(filtered_df)} remaining")

                companies_df = filtered_df

            filtered_out = original_count - len(companies_df)
            print(f"Ownership filters removed {filtered_out} companies")

        # Convert the string lists to a nicer format for display
        # This is optional but makes the output more readable
        for idx, row in companies_df.iterrows():
            # Format percentages with 2 decimal places
            if row[f'top_{top_percentages}_percentages'] and row[f'top_{top_percentages}_percentages'] != 'None':
                try:
                    percentages = eval(row[f'top_{top_percentages}_percentages'])
                    formatted = [f"{p:.2f}%" for p in percentages if p > 0]
                    companies_df.at[idx, f'top_{top_percentages}_percentages'] = ', '.join(formatted)
                except:
                    pass

            # Format owner names list
            if row[f'top_{top_names}_owners'] and row[f'top_{top_names}_owners'] != 'None':
                try:
                    owners = eval(row[f'top_{top_names}_owners'])
                    companies_df.at[idx, f'top_{top_names}_owners'] = ', '.join([o for o in owners if o])
                except:
                    pass

        # Save the results
        output_path = os.path.join(BASE_PATH, output_file)
        companies_df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"Saved {len(companies_df)} companies with ownership data to {output_path}")

        return companies_df

    except Exception as e:
        print(f"Error processing ownership data: {str(e)}")
        import traceback
        traceback.print_exc()
        return companies_df  # Return original data
