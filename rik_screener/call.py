log_step("ADDING INDUSTRY CLASSIFICATIONS")
industry_file = f"companies_with_industry_{years[-1]}_{years[0]}_{timestamp}.csv"

industry_df = add_industry_classifications(
    input_file=ratios_file,
    output_file=industry_file,
    revenues_file="revenues.csv",
    years=years
)

if industry_df is None:
    log_warning("Failed to add industry classifications. Using ratios file for next step")
    current_file = ratios_file
else:
    current_file = industry_file
    log_info(f"Columns in industry dataframe: {industry_df.columns.tolist()}")

log_step("ADDING COMPANY AGE")
age_file = f"companies_with_age_{years[-1]}_{years[0]}_{timestamp}.csv"

age_df = add_company_age(
    input_file=current_file,
    output_file=age_file,
    legal_data_file="legal_data.csv"
)

if age_df is None:
    log_warning("Failed to add company age. Using previous file for next step")
else:
    current_file = age_file
    log_info(f"Added company age successfully")

log_step("ADDING EMTAK DESCRIPTIONS")
emtak_file = f"companies_with_emtak_descriptions_{years[-1]}_{years[0]}_{timestamp}.csv"

emtak_df = add_emtak_descriptions(
    input_file=current_file,
    output_file=emtak_file,
    emtak_file="emtak_2008.csv",
    years=years,
    create_combined_columns=True
)

if emtak_df is None:
    log_warning("Failed to add EMTAK descriptions. Using previous file for next step")
else:
    current_file = emtak_file
    log_info(f"Added EMTAK descriptions successfully")

ranked_df = filter_and_rank(
    input_file=current_file,
    output_file=ranked_file,
    sort_column="score",
    filters=financial_filters,
    ascending=False,
    top_n=50,
    export_columns=export_columns
)

if ranked_df is None or ranked_df.empty:
    log_error("Failed to rank companies. Exiting")
    sys.exit()

log_step("ADDING COMPANY NAMES TO FINAL RESULTS")
final_file = f"final_companies_with_names_{years[-1]}_{years[0]}_{timestamp}.csv"

final_df = add_company_names(
    input_file=ranked_file,
    output_file=final_file,
    legal_data_file="legal_data.csv"
)

if final_df is None:
    log_warning("Failed to add company names. Using ranked file as final result")
    final_output = ranked_file
else:
    final_output = final_file
    log_info(f"Added company names successfully")

log_info("Top 10 companies after all filtering and ranking:")
if final_df is not None:
    log_info(str(final_df.head(10)))
else:
    log_info(str(ranked_df.head(10)))

log_info(f"Full results saved to: {config.get_file_path(final_output)}")
log_info("Analysis complete!")
