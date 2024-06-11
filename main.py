import os
import pandas as pd
import json

def load_mappings(file_path):
    with open(file_path, 'r') as f:
        mappings = json.load(f)
    return mappings

def extract_account_info(file_path):
    """
    Extract information from the pre-header rows in the CSV file.

    Parameters:
    file_path (str): The path to the CSV file.

    Returns:
    tuple: A tuple containing the pre-header information rows.
    """
    account_info = []
    with open(file_path, 'r') as file:
        for _ in range(4):
            account_info.append(file.readline().strip().split(','))

    return account_info[3]

def process_fnb_file(file_path):
    """
    Process the FNB file.

    Parameters:
    file_path (str): The path to the FNB CSV file.

    Returns:
    pd.DataFrame: Processed DataFrame with additional columns from pre-header rows.
    """
    # Extract pre-header information
    account_info = extract_account_info(file_path)
    acc_number = account_info[1]
    acc_name = account_info[2][2:-1]

    # Load the CSV file with the correct header row (4th row)
    df = pd.read_csv(file_path, header=4)

    # Remove any whitespace from the column names
    df.columns = df.columns.str.strip()

    # Add new columns to the DataFrame using the extracted values
    df["Account Number"] = acc_number
    df["Account Name"] = acc_name

    return df

def process_discovery_file(file_path):
    """
    Process the Discovery file.

    Parameters:
    file_path (str): The path to the Discovery CSV file.

    Returns:
    pd.DataFrame: Loaded DataFrame.
    """
    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Define the old and new column names
    old_new_columns = {
        'Value Date': 'Date',
        'Value Time': 'Time',
        'Type': 'Transaction Type',
        'Description': 'Description',
        'Beneficiary or CardHolder': 'Beneficiary/CardHolder',
        'Amount': 'Amount'
    }

    # Rename the columns
    df.rename(columns=old_new_columns, inplace=True)

    # Convert the Date column to datetime format and then format as YYYY/MM/DD
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y/%m/%d')

    # Add new columns to the DataFrame using the extracted values
    df["Account Number"] = "17275813806"
    df["Account Name"] = "Discovery Credit Card"

    return df

def process_standard_bank_file(file_path):
    """
    Process the Standard Bank file.

    Parameters:
    file_path (str): The path to the Standard Bank CSV file.

    Returns:
    pd.DataFrame: Loaded DataFrame.
    """
    # Define the custom header
    custom_header = ['HIST', 'Date', '#', 'Amount', 'Transaction Type', 'Description', 'Code', '0']

    # Load the CSV file starting from the correct row (line 3), and skip the last line
    df = pd.read_csv(file_path, skiprows=3, header=None, names=custom_header, skipfooter=1, engine='python')

    # Convert the Date column to datetime format and then format as YYYY/MM/DD
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.strftime('%Y/%m/%d')

    # Add new columns to the DataFrame using the extracted values
    df["Account Number"] = "428094465"
    df["Account Name"] = "Standard Bank"

    return df

def process_cash_file(file_path):
    """
    Process the Cash file.

    Parameters:
    file_path (str): The path to the Cash CSV file.

    Returns:
    pd.DataFrame: Loaded DataFrame.
    """
    # Load the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Add new columns to the DataFrame using the extracted values
    df["Account Number"] = "Cash Account"
    df["Account Name"] = "Cash Transactions"

    return df

def process_files_in_folder(folder_path, process_function):
    """
    Process all files in a folder using the specified processing function.

    Parameters:
    folder_path (str): The path to the folder containing CSV files.
    process_function (function): The function to process each file.

    Returns:
    pd.DataFrame: Concatenated DataFrame with all processed files.
    """
    dataframes = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            df = process_function(file_path)
            dataframes.append(df)
    if dataframes:
        concatenated_df = pd.concat(dataframes).drop_duplicates().reset_index(drop=True)
    else:
        concatenated_df = pd.DataFrame()  # Return an empty DataFrame if no CSV files are found
    return concatenated_df

def process_combined_dataframe(df, mappings):
    """
    Process the combined DataFrame and add a Category column based on Transaction Type and Description.

    Parameters:
    df (pd.DataFrame): Combined DataFrame containing transactions.
    mappings (dict): Mappings dictionary loaded from JSON file.

    Returns:
    pd.DataFrame: Processed DataFrame with added Category column.
    """
    # Format the combined Date column to YYYY/MM/DD
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y/%m/%d')

    # Get category mappings
    category_mapping = mappings.get('category_mapping', {})

    # Function to apply the category mapping loosely
    def apply_category(row):
        transaction_type = row['Transaction Type']
        description = row['Description']

        if not isinstance(description, str):
            description = str(description)  # Convert to string if not already

        description = description.strip().lower()  # Strip whitespace and convert to lowercase

        for key_transaction_type, key_descriptions in category_mapping.items():
            for key_description, category in key_descriptions.items():
                key_description = key_description.strip().lower()  # Strip whitespace and convert to lowercase

                if (key_transaction_type == '' or transaction_type == key_transaction_type) and key_description in description:
                    return category

        return 'Unknown'  # Default category if no match found

    # Apply the mapping to create the Category column
    df['Category'] = df.apply(apply_category, axis=1)

    # Select columns of interest and save to CSV
    df = df.loc[:, ['Date', 'Account Name', 'Account Number', 'Transaction Type', 'Description', 'Amount', 'Category']]
    #df = df.loc[:, ['Date', 'Description', 'Amount', 'Category', 'Account Name']]
    df.to_csv("Transactions.csv", index=False)

    return df

def transaction_statistics(df):
    # Group by Category and calculate count and total amount
    stats = df.groupby('Category').agg({'Amount': ['count', 'sum']})

    # Flatten the multi-index columns
    stats.columns = ['Count', 'Total Amount']

    # Sort by Count in descending order
    stats = stats.sort_values(by='Count', ascending=False)

    # Print the results
    print(f"{'Category':<20} {'Count':<10} {'Total Amount':<15}")
    print('-' * 45)
    for index, row in stats.iterrows():
        print(f"{index:<20} {row['Count']:<10} {row['Total Amount']:<15.2f}")

def main():
    # Define the folder paths
    fnb_folder_path = 'input/FNB'
    discovery_folder_path = 'input/Discovery'
    standard_bank_folder_path = 'input/Standard Bank'
    cash_folder_path = 'input/Cash'
    mappings = load_mappings('input/mappings.json')

    # Process each folder
    df_fnb = process_files_in_folder(fnb_folder_path, process_fnb_file)
    df_discovery = process_files_in_folder(discovery_folder_path, process_discovery_file)
    df_standard_bank = process_files_in_folder(standard_bank_folder_path, process_standard_bank_file)
    df_cash = process_files_in_folder(cash_folder_path, process_cash_file)

    # Merge the DataFrames
    all_dfs = [df_fnb, df_discovery, df_standard_bank, df_cash]
    combined_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates().reset_index(drop=True)
    processed_df = process_combined_dataframe(combined_df, mappings)

    transaction_statistics(processed_df)

    # Print the first few rows of relevant columns for verification where Category is "Unknown"
    unknown_df = processed_df.loc[processed_df['Category'] == 'Unknown', ["Transaction Type", "Description", "Date"]].sort_values(by='Date', ascending=False)
    print(unknown_df.head())

    return df_fnb, df_discovery, df_standard_bank, df_cash, processed_df

if __name__ == "__main__":
    df_fnb, df_discovery, df_standard_bank, df_cash, combined_df = main()
