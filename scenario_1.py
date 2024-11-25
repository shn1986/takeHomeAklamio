from airflow.utils.dates import days_ago
from db_operations import process_data
from airflow.decorators import task
from airflow import DAG
import pandas as pd
import functools
import datetime
import logging
import json

#Creating a generic exception handling function
def exception_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"An error occurred in {func.__name__}: {str(e)}")
            # Optionally, you could raise the exception again if you want it to propagate:
            # raise
            return None
    return wrapper
#This function will read and load the JSON data from a file.
@exception_handler
def load_data(filepath):    
    logging.info("Reading JSON data from {}".format(filepath))
    with open(filepath, 'r') as file:
        data = [json.loads(line) for line in file]
    return data

#This function will convert a list of dictionaries into a pandas DataFrame
@exception_handler
def convert_to_dataframe(data):
    logging.info("Converting JSON data into a Pandas DataFrame")
    return pd.DataFrame(data)

#This function will processe the DataFrame to convert 'fired_at' to datetime and adds 'date_hour' column.
@exception_handler
def preprocess_data(df):
    logging.info("Converting 'fired_at' to datetime and adding 'date_hour'")
    df['fired_at'] = pd.to_datetime(df['fired_at'], format='%m/%d/%Y, %H:%M:%S')
    df['date_hour'] = df['fired_at'].dt.strftime('%Y-%m-%d %H:00:00')
    return df

#This function will filter the DataFrame for specific event types.
@exception_handler
def filter_events(df, event_type):
    logging.info("Filtering data for '{}' events only".format(event_type))
    return df[df['event_type'] == event_type]

@exception_handler
def calculate_event_counts(df, group_by_columns):
    return df.groupby(group_by_columns).size().reset_index(name='event_count')

#This function will group the DataFrame by specified columns and counts the occurrences
@exception_handler
def group_and_count(df, group_by_columns, count_name):
    logging.info("Counting events per hour for each customer")
    counts = df.groupby(group_by_columns).size().reset_index(name=count_name)
    counts = counts.sort_values(by=group_by_columns).reset_index(drop=True)
    return counts

#This function will count unique users for specific events grouped by specified columns
@exception_handler
def count_unique_users(df, group_by_columns, unique_user_col):
    logging.info("Counting unique users for '{}' grouped by {}".format(unique_user_col, ", ".join(group_by_columns)))
    unique_users = df.groupby(group_by_columns)[unique_user_col].nunique().reset_index(name='unique_user_clicks')
    unique_users = unique_users.sort_values(by=group_by_columns).reset_index(drop=True)
    return unique_users

#Merging page loads and clicks data on the group_by_columns
@exception_handler
def calculate_click_through_rate(page_loads_df, clicks_df, group_by_columns):   
    merged_df = pd.merge(page_loads_df, clicks_df, on=group_by_columns, suffixes=('_page_load', '_click'))
    
    #Calculate click-through rate by comparing the number of clicks to page loads (This is my assumption)
    merged_df['click_through_rate'] = merged_df['event_count_click'] / merged_df['event_count_page_load']
    return merged_df

@task
def process_json_data():
    data = load_data('/opt/airflow/dags/flatfiles/aklamio_challenge.json')
    df = convert_to_dataframe(data)
    df = preprocess_data(df)
    
    #Filter events
    page_load_df = filter_events(df, 'ReferralPageLoad')
    recomend_click_df = filter_events(df, 'ReferralRecommendClick')
    
    #Count total events and unique users
    metrics = {
        'page_loads': group_and_count(page_load_df, ['customer_id', 'date_hour'], 'page_loads'),
        'clicks': group_and_count(recomend_click_df, ['customer_id', 'date_hour'], 'clicks'),
        'unique_user_clicks': count_unique_users(recomend_click_df, ['customer_id', 'date_hour'], 'user_id'),
    }
    
    #Calculate counts for each type and click-through rates
    page_counts = calculate_event_counts(page_load_df, ['customer_id', 'event_id'])
    clicks_counts = calculate_event_counts(recomend_click_df, ['customer_id', 'event_id'])
    click_through_rates = calculate_click_through_rate(page_counts, clicks_counts, ['customer_id', 'event_id'])

    #Define a common mapping for customer_id and date_hour
    common_mapping = {
        'customer_id': 'customer_id',
        'date_hour': 'date_hour'
    }

    #Process data for metrics with additional specific mappings.
    process_data(metrics['page_loads'], 'page_loads', {**common_mapping, 'page_loads': 'page_loads'}, ['customer_id', 'date_hour'])
    process_data(metrics['clicks'], 'clicks', {**common_mapping, 'clicks': 'clicks'}, ['customer_id', 'date_hour'])
    process_data(metrics['unique_user_clicks'], 'unique_user_clicks', {**common_mapping, 'unique_user_clicks': 'unique_user_clicks'}, ['customer_id', 'date_hour'])
    
    #Handle click-through rate data with specific event-based mapping
    click_through_rate_mapping = {
        'customer_id': 'customer_id',
        'event_id': 'event_id',
        'event_count_page_load': 'event_count_page_load',
        'event_count_click': 'event_count_click',
        'click_through_rate': 'click_through_rate'
    }
    process_data(click_through_rates, 'click_through_rate', click_through_rate_mapping, ['customer_id', 'event_id'])


#Define default DAG arguments
default_args = {
    'owner': 'aklamio',
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=5),
}

#Define the DAG
with DAG(
    'process_events_dag_scenario_1',
    default_args=default_args,
    description='Process campaign events in memory and update metrics',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['data-pipeline', 'campaign'],
) as dag:


    db_update = process_json_data()

    db_update