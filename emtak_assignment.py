import pandas as pd
import io
from google.colab import files
import ipywidgets as widgets
from IPython.display import display

def replace_industry_codes(company_data_file, emtak_file, industry_code_column):
    try:
        company_df = pd.read_csv(company_data_file, encoding='utf-8')
    except UnicodeDecodeError:
        company_data_file.seek(0)
        company_df = pd.read_csv(company_data_file, encoding='latin1')

    try:
        emtak_df = pd.read_csv(emtak_file, encoding='utf-8')
    except UnicodeDecodeError:
        emtak_file.seek(0)
        emtak_df = pd.read_csv(emtak_file, encoding='latin1')

    if emtak_df.shape[1] != 2:
        emtak_file.seek(0)
        emtak_df = pd.read_csv(emtak_file, header=None, encoding='utf-8')
        if emtak_df.shape[1] != 2:
            raise ValueError("EMTAK file must have exactly 2 columns")

    emtak_df.columns = ['code', 'description'] if emtak_df.shape[1] == 2 else emtak_df.columns

    if industry_code_column not in company_df.columns:
        raise ValueError(f"Column '{industry_code_column}' not found in company data")

    company_df[industry_code_column] = company_df[industry_code_column].fillna('').astype(str)
    emtak_df['code'] = emtak_df['code'].astype(str)

    company_df[industry_code_column] = company_df[industry_code_column].apply(
        lambda x: x.split('.')[0] if x and '.' in x else x
    )

    new_column_name = industry_code_column.replace('code', 'description')
    if new_column_name == industry_code_column:
        new_column_name = f"{industry_code_column}_description"

    combined_column_name = f"{industry_code_column}_with_description"

    emtak_dict = dict(zip(emtak_df['code'], emtak_df['description']))
    company_df[new_column_name] = company_df[industry_code_column].map(emtak_dict)

    company_df[combined_column_name] = company_df.apply(
        lambda row: f"{row[industry_code_column]} - {row[new_column_name]}"
        if pd.notna(row[new_column_name]) and row[industry_code_column] != ''
        else row[industry_code_column],
        axis=1
    )

    mapped_count = company_df[new_column_name].notna().sum()
    total_count = len(company_df)
    print(f"Successfully mapped {mapped_count} out of {total_count} industry codes to descriptions")

    unmapped_codes = company_df.loc[company_df[new_column_name].isna() & (company_df[industry_code_column] != ''),
                                    industry_code_column].unique()
    if len(unmapped_codes) > 0:
        print(f"Warning: {len(unmapped_codes)} industry codes could not be mapped")
        if len(unmapped_codes) <= 10:
            print(unmapped_codes)
        else:
            print("First 10 unmapped codes:", unmapped_codes[:10])

    return company_df

def run_tool():
    print("Upload your company data CSV file:")
    uploaded_company_file = files.upload()
    company_filename = list(uploaded_company_file.keys())[0]
    company_file = io.BytesIO(uploaded_company_file[company_filename])

    print("\nUpload your EMTAK codes CSV file:")
    uploaded_emtak_file = files.upload()
    emtak_filename = list(uploaded_emtak_file.keys())[0]
    emtak_file = io.BytesIO(uploaded_emtak_file[emtak_filename])

    print("\nEnter the name of the column containing industry codes:")
    column_name_input = widgets.Text(
        value='industry_code',
        description='Column name:',
    )
    display(column_name_input)

    process_button = widgets.Button(description="Process Files")
    display(process_button)

    def on_process_button_clicked(b):
        column_name = column_name_input.value.strip()
        if not column_name:
            print("Error: Please provide a column name")
            return

        try:
            result_df = replace_industry_codes(company_file, emtak_file, column_name)

            output_filename = f"{company_filename.split('.')[0]}_with_descriptions.csv"
            result_df.to_csv(output_filename, index=False, encoding='utf-8-sig')

            print(f"\nResult saved as '{output_filename}'")
            print("Downloading the result file...")
            files.download(output_filename)
        except Exception as e:
            print(f"Error: {str(e)}")

    process_button.on_click(on_process_button_clicked)

print("Industry Code Lookup and Replacement Tool")
print("----------------------------------------")
run_tool()
