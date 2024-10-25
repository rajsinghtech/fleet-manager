import streamlit as st
import boto3
from botocore.client import Config
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timezone
import os  # Import os to access environment variables

# Load AWS credentials and configuration from environment variables
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME')
AWS_BUCKET_REGION = os.environ.get('AWS_BUCKET_REGION')
AWS_BUCKET_HOST = os.environ.get('AWS_BUCKET_HOST')
AWS_BUCKET_PORT = os.environ.get('AWS_BUCKET_PORT')
AWS_BUCKET_PROTOCOL = os.environ.get('AWS_BUCKET_PROTOCOL')

# Construct the endpoint URL
endpoint_url = f"{AWS_BUCKET_PROTOCOL}://{AWS_BUCKET_HOST}:{AWS_BUCKET_PORT}"

# S3 client configuration with adjustments
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=endpoint_url,
    region_name=AWS_BUCKET_REGION,
    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
)

def list_prefixes(client, bucket_name, prefix='', delimiter='/'):
    paginator = client.get_paginator('list_objects_v2')
    prefixes = set()
    for result in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter):
        for prefix in result.get('CommonPrefixes', []):
            prefixes.add(prefix.get('Prefix').rstrip('/'))
    return sorted(list(prefixes))

bucket_name = AWS_BUCKET_NAME

st.title('S3 JSON Data Visualization')

# Get the list of VINs
vins = [prefix.split('/')[0] for prefix in list_prefixes(s3_client, bucket_name, prefix='', delimiter='/')]

if not vins:
    st.error("No VINs found in the bucket. Please check your S3 connection and bucket contents.")
    st.stop()

selected_vin = st.selectbox('Select VIN', vins)

if selected_vin:
    # Now, list available years for the selected VIN
    vin_prefix = f"{selected_vin}/"
    years = [prefix.split('/')[-1] for prefix in list_prefixes(s3_client, bucket_name, prefix=vin_prefix, delimiter='/')]
    selected_year = st.selectbox('Select Year', years)
    
    if selected_year:
        year_prefix = f"{vin_prefix}{selected_year}/"
        months = [prefix.split('/')[-1] for prefix in list_prefixes(s3_client, bucket_name, prefix=year_prefix, delimiter='/')]
        selected_month = st.selectbox('Select Month', months)
        
        if selected_month:
            month_prefix = f"{year_prefix}{selected_month}/"
            days = [prefix.split('/')[-1] for prefix in list_prefixes(s3_client, bucket_name, prefix=month_prefix, delimiter='/')]
            selected_day = st.selectbox('Select Day', days)
            
            if selected_day:
                # Now, we can list the JSON files for this VIN and date
                day_prefix = f"{month_prefix}{selected_day}/"
                # List all JSON files under this prefix
                paginator = s3_client.get_paginator('list_objects_v2')
                json_files = []
                for result in paginator.paginate(Bucket=bucket_name, Prefix=day_prefix):
                    for obj in result.get('Contents', []):
                        key = obj['Key']
                        if key.endswith('.json'):
                            json_files.append(key)
                st.write(f"Found {len(json_files)} JSON files for VIN {selected_vin} on {selected_year}-{selected_month}-{selected_day}")
                
                # Add a progress bar
                progress_bar = st.progress(0)
                
                # Now, process the JSON files
                data_list = []
                total_files = len(json_files)
                for idx, json_file in enumerate(json_files):
                    obj = s3_client.get_object(Bucket=bucket_name, Key=json_file)
                    json_content = obj['Body'].read().decode('utf-8')
                    json_data = json.loads(json_content)
                    
                    # Extract 'created_at' and 'data'
                    created_at = json_data.get('created_at')
                    if created_at:
                        # Convert 'created_at' to datetime using timezone-aware method
                        timestamp = created_at.get('seconds', 0) + created_at.get('nanos', 0) / 1e9
                        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    else:
                        continue
                    data_entries = json_data.get('data', [])
                    for entry in data_entries:
                        key = entry.get('key')
                        value_dict = entry.get('value', {}).get('Value', {})
                        val = None  # Initialize val
                        if 'StringValue' in value_dict:
                            val = value_dict['StringValue']
                            data_list.append({'datetime': dt, 'key': key, 'value': val})
                        elif 'LocationValue' in value_dict:
                            location = value_dict['LocationValue']
                            lat = location.get('latitude')
                            lon = location.get('longitude')
                            data_list.append({'datetime': dt, 'key': f"{key}_lat", 'value': lat})
                            data_list.append({'datetime': dt, 'key': f"{key}_lon", 'value': lon})
                        elif 'Invalid' in value_dict and value_dict['Invalid']:
                            val = None
                            data_list.append({'datetime': dt, 'key': key, 'value': val})
                        else:
                            val = None
                            data_list.append({'datetime': dt, 'key': key, 'value': val})
                    
                    # Update progress bar
                    progress = (idx + 1) / total_files
                    progress_bar.progress(progress)
                
                if data_list:
                    df = pd.DataFrame(data_list)
                    st.write("### Raw Dataframe")
                    st.write(df)
                    
                    # Convert 'key' to string
                    df['key'] = df['key'].astype(str)
                    
                    # Convert 'value' to numeric if possible
                    df['value_numeric'] = pd.to_numeric(df['value'], errors='coerce')
                    
                    unique_keys = df['key'].unique()
                    selected_keys = st.multiselect('Select keys to plot', unique_keys)
                    if selected_keys:
                        for key in selected_keys:
                            df_key = df[df['key'] == key].copy()
                            df_key.sort_values('datetime', inplace=True)
                            
                            st.write(f"### Data for key {key}")
                            st.write(df_key)
                            
                            if df_key['value_numeric'].notnull().any():
                                df_key.set_index('datetime', inplace=True)
                                st.line_chart(df_key['value_numeric'])
                            else:
                                st.write(f"Key {key} has non-numeric values or no valid data to plot.")
                                st.write(df_key[['datetime', 'value']])
                    else:
                        st.warning("Please select at least one key to plot.")
                else:
                    st.warning("No data available to display.")
            else:
                st.warning("Please select a day.")
        else:
            st.warning("Please select a month.")
    else:
        st.warning("Please select a year.")
else:
    st.warning("Please select a VIN.")