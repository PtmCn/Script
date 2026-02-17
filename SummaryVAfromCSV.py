import pandas as pd
import os
import glob
import sys

# --- 1. Setup: Define the list of input files and the output name/format ---

# Uses glob to find all CSV files in the current directory starting with 'title'
INPUT_FILES = glob.glob('dusitcentralpark_va/dusit*.csv')

OUTPUT_FILE = 'output/dusitcentralpark_com_summary_VAQ4.csv' # Define the output file name

try:
    # --- 2. Read & Combine: Load all CSV data into a single Pandas DataFrame ---
    all_dfs = []
    
    print("Starting data processing...")
    
    for file_name in INPUT_FILES:
        # Check if the file exists before attempting to read it
        if not os.path.exists(file_name):
            print(f"Warning: Input file not found: {file_name}. Skipping.")
            continue
            
        # Read each individual file
        df_part = pd.read_csv(file_name)
        # Optional: Add a column to know which file the data came from
        df_part['SourceFile'] = file_name
        all_dfs.append(df_part)
        
    if not all_dfs:
        print("Error: No input files were successfully read. Exiting.")
        sys.exit(1) # Exit the script if no files were read
    else:
        # Concatenate all DataFrames into one master DataFrame
        df = pd.concat(all_dfs, ignore_index=True)
        
        print(f"\nSuccessfully combined {len(all_dfs)} input files. Total rows: {len(df)}")
        
        # --- NEW LOGIC: DEDUPLICATE INSTANCES ---
        initial_rows = len(df)
        
        # Deduplicate rows based on the combination of Name, Host, and Port.
        # This prevents the same vulnerability on the same port on the same host
        # from being counted multiple times.
        # If 'Port' column is missing, this will raise a KeyError.
        df = df.drop_duplicates(subset=['Name', 'Host', 'Port'], keep='first')
        
        deduplicated_rows = len(df)
        print(f"Deduplication applied: {initial_rows - deduplicated_rows} duplicate rows removed.")
        # --- END DEDUPLICATION LOGIC ---
        
        print("\n--- Combined Raw Data (First 5 rows) ---")
        print(df.head())
        
        # --- 3. Query (Filtering & Aggregation) ---
        
        # Filter rows where the 'Risk' column value is in the target_risks list
        target_risks = ['Critical', 'High', 'Medium']
        filtered_df = df[df['Risk'].isin(target_risks)].copy() # Use .copy() to avoid SettingWithCopyWarning
        
        if filtered_df.empty:
            print("\nNo data remaining after filtering. Exiting.")
            # Create an empty file with headers before exiting
            empty_df = pd.DataFrame(columns=['Name', 'Risk', 'Total_Count', 'Host', 'Per_Host_Count'])
            empty_df.to_csv(OUTPUT_FILE, index=False)
            sys.exit(0) # Exit the script successfully after writing an empty file

        # 3a. Generate the Host Detail Data (The specific counts for each host)
        # Group by Name, Risk, and Host, and count how many times it appears (Per_Host_Count)
        detail_df = filtered_df.groupby(['Name', 'Risk', 'Host']).size().reset_index(name='Per_Host_Count')
        
        # 3b. Generate the Summary Data (The overall counts)
        # Group by Name and Risk, and get the Total Count and Unique Host Count
        summary_df = filtered_df.groupby(['Name', 'Risk']).agg(
            Total_Count=('Risk', 'size'),
        ).reset_index()

        # 3c. Merge the two DataFrames
        # Join the detailed counts with the overall summary counts on Name and Risk.
        report_df = pd.merge(detail_df, summary_df, on=['Name', 'Risk'], how='left')


        # --- SORTING BY RISK PRIORITY (CRITICAL > HIGH > MEDIUM) ---
        
        # 1. Define the custom risk order for sorting
        risk_order = ['Critical', 'High', 'Medium']

        # 2. Convert the 'Risk' column to a categorical type with the specified order
        risk_cat = pd.CategoricalDtype(categories=risk_order, ordered=True)
        report_df['Risk'] = report_df['Risk'].astype(risk_cat)

        # 3. Sort the DataFrame: first by 'Risk', then by 'Total_Count' (descending), 
        #    and finally by 'Name' for stable grouping.
        report_df = report_df.sort_values(
            by=['Risk', 'Total_Count', 'Name'], 
            ascending=[True, False, True]
        )

        # Reorder columns to match the desired output structure
        report_df = report_df[[
            'Name', 
            'Risk', 
            'Total_Count',
            'Host',
            'Per_Host_Count'
        ]]


        print("\n--- Filtered, Grouped, and Sorted Detailed Data ---")
        print(report_df)

        # --- 4. Output: Save the resulting DataFrame to a new CSV file ---
        
        # index=False prevents Pandas from writing the DataFrame index as a column
        report_df.to_csv(OUTPUT_FILE, index=False)
        
        print(f"\n========================================================")
        print(f"OUTPUT REPORT SUCCESSFULLY SAVED to file: {OUTPUT_FILE}")
        print("========================================================")
        
except Exception as e:
    print(f"An unexpected error occurred during processing: {e}")