import streamlit as st
import requests
import pandas as pd
import numpy as np


import boto3
from datetime import date


aws_access_key_id = st.secrets["aws_access_key_id"]
aws_secret_access_key = st.secrets["aws_secret_access_key"]
account_id = st.secrets["account_id"]

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

quicksight_client = session.client('quicksight',region_name='us-east-1')
    
import json

next_token = None
output_columns_by_dataset = {}  # Initialize an empty dictionary to store output columns by dataset ARN

next_token = None
output_columns_by_dataset = {}  # Initialize an empty dictionary to store output columns by dataset ARN
dataset_arns = []  # List to store dataset ARNs
dataset_names = []  # List to store dataset names
output_columns_list = []  # List to store output columns

while True:
    try:
        if next_token:
            lds_response = quicksight_client.list_data_sets(AwsAccountId=account_id, NextToken=next_token, MaxResults=99)
        else:
            lds_response = quicksight_client.list_data_sets(AwsAccountId=account_id, MaxResults=99)

        for i in lds_response['DataSetSummaries']:
            dataset_id = i['DataSetId']
            try:
                dds_response = quicksight_client.describe_data_set(AwsAccountId=account_id, DataSetId=dataset_id)
                dataset_name = dds_response['DataSet']['Name'] 
                dataset_arn = dds_response['DataSet']['Arn']  # Get the dataset ARN from the dds_response
                print(dataset_arn,dataset_name)

                if 'DataSet' in dds_response and 'OutputColumns' in dds_response['DataSet']:
                    output_columns = dds_response['DataSet']['OutputColumns']
                    if output_columns:
                        column_names = {column.get('Name') for column in output_columns}  # Convert to set
                        output_columns_by_dataset[dataset_arn] = (dataset_name, column_names)
                    else:
                        output_columns_by_dataset[dataset_arn] = {dataset_name: column_names}  # Create a new dictionary for dataset_name and column_names

            except Exception as e:
                print(f"Error describing dataset {dataset_id}: {str(e)}")

        if 'NextToken' in lds_response:
            next_token = lds_response['NextToken']
        else:
            break

    except Exception as e:
        print(f"Error listing data sets: {str(e)}")
        break

# Now 'output_columns_by_dataset' contains the "Output Columns" for each dataset ARN as sets
#print(output_columns_by_dataset)

columns_by_dataset_arn = {}  # Initialize an empty dictionary to store columns used by dataset ARN

def list_all_analyses(quicksight_client, account_id):
    analyses = []
    next_token = None
    
    while True:
        if next_token:
            response = quicksight_client.list_analyses(AwsAccountId=account_id, NextToken=next_token, MaxResults=99)
        else:
            response = quicksight_client.list_analyses(AwsAccountId=account_id, MaxResults=99)
        
        analyses.extend(response['AnalysisSummaryList'])
        for i in analyses:
            print(i['Name'],i['AnalysisId'])
            
        
        if 'NextToken' in response:
            next_token = response['NextToken']
        else:
            break
    
    return analyses

def get_all_values_by_key(json_obj, key):
    values = set()
    
    def extract_values(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == key:
                    values.add(v)
                elif isinstance(v, (dict, list)):
                    extract_values(v)
        elif isinstance(obj, list):
            for item in obj:
                extract_values(item)
    
    extract_values(json_obj)
    return values


def describe_analysis(quicksight_client, account_id, analysis_id):
    response = quicksight_client.describe_analysis_definition(AwsAccountId=account_id, AnalysisId=analysis_id)
    #print(response['Definition'])
    return response['Definition']


columns_by_dataset_arn = {}  # Initialize an empty dictionary to store output columns by dataset ARN
dataset_arns_list = []  # List to store dataset ARNs
analysis_names_list = []  # List to store analysis names
output_columns_list = []  # List to store output columns

def main():
    # Call the function to retrieve all analyses
    all_analyses = list_all_analyses(quicksight_client, account_id)

    for analysis in all_analyses:
        analysis_id = analysis['AnalysisId']
        name = analysis['Name']
        
        # Call the function to describe the analysis
        analysis_details = describe_analysis(quicksight_client, account_id, analysis_id)
        
        # Extract the dataset ARNs used in the analysis
        dataset_arns = get_all_values_by_key(analysis_details, "DataSetArn")
        
        # Process the dataset ARNs and add used columns to the dictionary
        for dataset_arn in dataset_arns:
            if dataset_arn not in columns_by_dataset_arn:
                columns_by_dataset_arn[dataset_arn] = set()
            
            # Extract the column names used in the analysis
            column_names = get_all_values_by_key(analysis_details, "ColumnName")
            
            # Add the column names to the dataset ARN's set of used columns
            columns_by_dataset_arn[dataset_arn].update(column_names)
            
            # Store dataset ARN, analysis name, and output columns in lists
            dataset_arns_list.append(dataset_arn)
            analysis_names_list.append(name)
            output_columns_list.append(column_names)

if __name__ == '__main__':
    main()


df2_data = [(dataset_arn, analysis_name, columns) for dataset_arn, analysis_name, columns in zip(dataset_arns_list, analysis_names_list, output_columns_list)]
df2 = pd.DataFrame(df2_data, columns=['dataset_arn', 'analysis_name', 'columns_arn'])
df1_data = [(dataset_arn, dataset_name, columns) for dataset_arn, (dataset_name, columns) in output_columns_by_dataset.items()]
df1 = pd.DataFrame(df1_data, columns=['dataset_arn', 'dataset_name', 'columns_output'])


# Merge dataframes on 'dataset_arn'
merged_df = pd.merge(df1, df2, on='dataset_arn', how='outer')

# Replace NaN values with empty sets
merged_df['columns_output'] = merged_df['columns_output'].apply(lambda x: set() if pd.isna(x) else x)
merged_df['columns_arn'] = merged_df['columns_arn'].apply(lambda x: set() if pd.isna(x) else x)

# Find the differences and create a new column 'difference' to store the changes
#merged_df['difference'] = merged_df.apply(lambda row: row['columns_output'].difference( row['columns_arn'], axis=1)
merged_df['difference'] = merged_df.apply(lambda row: row['columns_output'].difference(row['columns_arn']), axis=1)


# Filter rows where there are differences
differences_df = merged_df[merged_df['difference'].apply(lambda x: len(x) > 0)]
    


def load_lottieurl(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()


# Use local CSS
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)



# ---- HEADER SECTION ----
with st.container():
    st.subheader("Hi, I am Dinesh :wave:")
    st.title("A Data Analyst")
    

# ---- WHAT I DO ----
with st.container():
    st.write("---")
    left_column, right_column = st.columns(2)
    with left_column:
        st.header("What I do")
        st.write("##")
        st.write(
            """ I have             
            """
        )

# List of columns you want to display
columns_to_display = ['dataset_arn','dataset_name' ,'analysis_name','difference']

# Subsetting the DataFrame to include only the selected columns
selected_columns_df = differences_df[columns_to_display]

st.dataframe(selected_columns_df )
