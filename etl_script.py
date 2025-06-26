import pandas as pd
from sqlalchemy import create_engine
def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"Data extracted successfully from {file_path}")  
        return df
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None
def transform_data(df):
    if df is not None:
        df = df.drop(columns=['Lat', 'Long', 'Province/State']).groupby('Country/Region').sum().reset_index()
        df=df.melt(id_vars=['Country/Region'],var_name='Date', value_name='Count')
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%y')
        df=df.sort_values(by=['Country/Region', 'Date'])
        df['daily_change'] = df.groupby('Country/Region')['Count'].diff().fillna(0)
        return df
    else:
        print("No data to transform.")
        return None
# Extract the data
confirmed_data_df = extract_data('data/time_series_covid19_confirmed_global.csv')
deaths_data_df = extract_data('data/time_series_covid19_deaths_global.csv')
recoveries_data_df = extract_data('data/time_series_covid19_recovered_global.csv')  
# Transform the data
confirmed_data_transformed = transform_data(confirmed_data_df)
deaths_data_transformed = transform_data(deaths_data_df)
recoveries_data_transformed = transform_data(recoveries_data_df)
# Change column names
confirmed_data_transformed.columns = ['country','date','accumulated_confirmed','increase_of_confirmed']
recoveries_data_transformed.columns = ['country','date','accumulated_recovered','increase_of_recovered']
deaths_data_transformed.columns = ['country','date','accumulated_deaths','increase_of_deaths']


# Extract csv to clean data
# confirmed_data_transformed.to_csv('data/cleaned_confirmed_data.csv', index=False)
# deaths_data_transformed.to_csv('data/cleaned_deaths_data.csv', index=False)
# recoveries_data_transformed.to_csv('data/cleaned_recoveries_data.csv', index=False)
# load the data
user = 'postgres'
password = 'secret'
host = 'localhost'
port = '5432'
database = 'mydb'
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
confirmed_data_transformed.to_sql(name='confirmed', con=engine, if_exists='replace', index=False)
recoveries_data_transformed.to_sql(name='recovered', con=engine, if_exists='replace', index=False)
deaths_data_transformed.to_sql(name='death', con=engine, if_exists='replace', index=False)
#analysis
# Top 5 countries with the most confirmed cases
most_confirmed = pd.read_sql("""
                                SELECT country, sum(accumulated_confirmed) AS top_confirmed
                                FROM confirmed
                                GROUP BY country
                                ORDER BY sum(accumulated_confirmed) DESC
                                LIMIT 5;
                             """, con=engine)
print("Top 5 countries with the most confirmed cases:", most_confirmed)

