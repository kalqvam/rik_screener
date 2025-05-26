def add_industry_classifications(
    input_file: str = "companies_with_ratios.csv",
    output_file: str = "companies_with_industry.csv",
    revenues_file: str = "revenues.csv",
    years: list = None
) -> pd.DataFrame:
    print(f"\n=== STEP 2.5: ADDING INDUSTRY CLASSIFICATIONS ===")
    print(f"Loading companies from {input_file}...")

    input_path = os.path.join(BASE_PATH, input_file)
    if not os.path.exists(input_path):
        print(f"Error: Input file {input_path} not found")
        return None

    companies_df = pd.read_csv(input_path)
    print(f"Loaded {len(companies_df)} companies")

    # Set default years if none provided
    if years is None:
        years = [2023, 2022, 2021]  # Default years

    # Ensure years are sorted (most recent first)
    years = sorted(years, reverse=True)

    # Load the revenue by industry file
    revenues_path = os.path.join(BASE_PATH, revenues_file)
    if not os.path.exists(revenues_path):
        print(f"Error: Revenue file {revenues_path} not found")
        return companies_df  # Return original data without industry codes

    print(f"Loading industry revenue data from {revenues_file}...")

    # Read the headers first to confirm column names
    header_data = pd.read_csv(
        revenues_path,
        nrows=0,
        encoding="utf-8",
        sep=";"
    )
    print(f"Available columns in revenues file: {header_data.columns.tolist()}")

    # Process each year
    for year in years:
        report_id_col = f"report_id_{year}"

        # Skip years without report_id column
        if report_id_col not in companies_df.columns:
            print(f"Warning: {report_id_col} not found in companies data. Skipping year {year}.")
            continue

        print(f"Processing industry classifications for year {year}...")

        # Get all report_ids for this year that we need to look up
        report_ids = companies_df[report_id_col].dropna().astype(int).tolist()
        print(f"Found {len(report_ids)} report IDs to process for year {year}")

        if not report_ids:
            continue

        # Read revenue data in chunks to handle large files efficiently
        chunks = []
        chunk_size = 500000  # Adjust based on memory constraints

        try:
            for chunk in pd.read_csv(
                revenues_path,
                chunksize=chunk_size,
                encoding="utf-8",
                sep=";",
                dtype={"report_id": int, "emtak": str}  # Ensure proper types
            ):
                # Filter for main industry codes ("põhitegevusala" = "jah") and our report_ids
                filtered_chunk = chunk[
                    (chunk["report_id"].isin(report_ids)) &
                    (chunk["põhitegevusala"] == "jah")
                ]

                if not filtered_chunk.empty:
                    chunks.append(filtered_chunk)

            if not chunks:
                print(f"No industry data found for year {year}")
                continue

            # Combine chunks and keep only report_id and emtak columns
            industry_data = pd.concat(chunks, ignore_index=True)
            industry_data = industry_data[["report_id", "emtak"]]

            print(f"Found {len(industry_data)} industry classifications for year {year}")

            # Check for duplicates (should not happen since we filtered for põhitegevusala="jah")
            duplicates = industry_data["report_id"].duplicated()
            if duplicates.any():
                dupe_count = duplicates.sum()
                print(f"Warning: Found {dupe_count} duplicate main industry codes. Using the first occurrence.")
                industry_data = industry_data.drop_duplicates(subset="report_id", keep="first")

            # Rename emtak column to include year
            industry_data = industry_data.rename(columns={"emtak": f"industry_code_{year}"})

            # Merge with companies data
            companies_df = pd.merge(
                companies_df,
                industry_data,
                left_on=report_id_col,
                right_on="report_id",
                how="left"
            )

            # Drop the duplicate report_id column from the merge
            if "report_id" in companies_df.columns:
                companies_df = companies_df.drop(columns=["report_id"])

            # Count how many companies got an industry code
            assigned_count = companies_df[f"industry_code_{year}"].notna().sum()
            print(f"Assigned industry codes to {assigned_count} out of {len(companies_df)} companies for year {year}")

        except Exception as e:
            print(f"Error processing industry data for year {year}: {str(e)}")
            import traceback
            traceback.print_exc()

    # Save the results
    output_path = os.path.join(BASE_PATH, output_file)
    companies_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved {len(companies_df)} companies with industry codes to {output_path}")

    # Print unique industry codes found (for the most recent year)
    for year in years:
        industry_col = f"industry_code_{year}"
        if industry_col in companies_df.columns:
            unique_codes = companies_df[industry_col].dropna().unique()
            print(f"\nFound {len(unique_codes)} unique industry codes for {year}")
            print("Sample of industry codes:", unique_codes[:10])
            break

    return companies_df
