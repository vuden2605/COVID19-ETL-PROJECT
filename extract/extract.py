import pandas as pd
def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"Data extracted successfully from {file_path}")  
        return df
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None
confirmed_data_df = extract_data('data/time_series_covid19_confirmed_global.csv')
deaths_data_df = extract_data('data/time_series_covid19_deaths_global.csv')
recoveries_data_df = extract_data('data/time_series_covid19_recovered_global.csv')  
print(confirmed_data_df.head())
print(deaths_data_df.head())
print(recoveries_data_df.head())