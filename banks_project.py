import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3


data_url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_csv_path = "exchange_rate (1).csv"
output_csv_path = "./Largest_banks_data.csv"
database_name = "Banks.db"
table_name = "Largest_banks"
log_file_path = "code_log.txt"


# Task 1: Function to log progress
def log_progress(log_points):
    with open(log_file_path, 'a') as log_file:
        for point in log_points:
            log_file.write(f"{point}\n")


# Task 2: Function to extract tabular information
def extract():
    log_progress(["Task 2: Extracting tabular information"])

    response = requests.get(data_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Identify the position and pattern of the tabular information
    table = soup.find('table', class_='wikitable')
    print(table)

    # Create a DataFrame
    df = pd.read_html(str(table))[0]

    log_progress(["Task 2: Extraction completed"])

    return df


def transform(df, exchange_rate_csv_path):
    log_progress(["Task 3: Transforming the dataframe"])

    # Load exchange rates from CSV
    exchange_rates = pd.read_csv(exchange_rate_csv_path)

    # Check if 'MC_USD_Billion' exists in the DataFrame
    if 'MC_USD_Billion' not in df.columns:
        log_progress(["Error: 'MC_USD_Billion' column not found in the DataFrame"])
        return df

    # Perform currency conversion
    currencies = ['GBP', 'EUR', 'INR']
    for currency in currencies:
        df[f'MC_{currency}_Billion'] = round(df['MC_USD_Billion'] * exchange_rates[f'USD_to_{currency}'], 2)

    log_progress(["Task 3: Transformation completed"])

    return df


# Task 4: Function to load to CSV
def load_to_csv(df):
    log_progress(["Task 4: Loading to CSV"])

    # Save the DataFrame to a CSV file
    df.to_csv(output_csv_path, index=False)

    log_progress(["Task 4: CSV loading completed"])


# Task 5: Function to load to SQL database
def load_to_db(df):
    log_progress(["Task 5: Loading to SQL database"])

    # Save the DataFrame to a SQLite database
    conn = sqlite3.connect(database_name)
    df.to_sql(table_name, conn, index=False, if_exists='replace')
    conn.close()

    log_progress(["Task 5: Database loading completed"])


# Task 6: Function to run queries on the database
def run_queries(queries):
    log_progress(["Task 6: Running queries on the database"])

    # Connect to the SQLite database
    conn = sqlite3.connect(database_name)

    # Execute the given set of queries
    for query in queries:
        result = pd.read_sql_query(query, conn)
        print(result)

    # Close the database connection
    conn.close()

    log_progress(["Task 6: Queries executed"])


# Task 7: Function to verify log entries
def verify_log():
    log_progress(["Task 7: Verifying log entries"])

    # Read the contents of the log file
    with open(log_file_path, 'r') as log_file:
        print(log_file.read())

    log_progress(["Task 7: Log verification completed"])


# Main function to execute all tasks
def main():
    # Task 1: Log progress at the start
    log_progress(["Task 1: Start of the script"])

    # Task 2: Extract tabular information
    df = extract()

    # Task 3: Transform the dataframe
    df_transformed = transform(df, exchange_rate_csv_path)

    # Task 4: Load to CSV
    load_to_csv(df_transformed)

    # Task 5: Load to SQL database
    load_to_db(df_transformed)

    # Task 6: Run queries on the database
    queries_to_run = ["SELECT * FROM Largest_banks LIMIT 5;", "SELECT COUNT(*) FROM Largest_banks;"]
    run_queries(queries_to_run)

    # Task 7: Verify log entries
    verify_log()


if __name__ == "__main__":
    main()
