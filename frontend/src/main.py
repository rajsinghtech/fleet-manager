import streamlit as st
import boto3
from botocore.client import Config
import json
import pandas as pd
import plotly.express as px
from datetime import datetime, timezone
import os  # Import os to access environment variables
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure Streamlit page
st.set_page_config(
    page_title="Fleet Manager",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Add a logo and title
def add_header():
    col1, col2 = st.columns([1, 3])
    with col1:
        # Replace 'logo.png' with the path to your logo image
        if os.path.exists("frontend/src/logo.png"):
            st.image("frontend/src/logo.png", width=100)
    with col2:
        st.title("Fleet Manager 🚗")
        st.markdown("### Monitor and Analyze Your Tesla Fleet Telemetry")

add_header()

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

@st.cache_resource
def get_s3_client():
    """Initialize and cache the S3 client."""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=endpoint_url,
        region_name=AWS_BUCKET_REGION,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
    )

s3_client = get_s3_client()

@st.cache_data
def list_prefixes(_client, bucket_name, prefix='', delimiter='/'):
    """
    List unique prefixes (e.g., VINs, years, months, days) in the specified S3 bucket.

    Args:
        _client: Boto3 S3 client.
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix to filter the objects.
        delimiter (str): Delimiter for grouping keys.

    Returns:
        List of sorted unique prefixes.
    """
    paginator = _client.get_paginator('list_objects_v2')
    prefixes = set()
    try:
        for result in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter):
            for prefix_item in result.get('CommonPrefixes', []):
                prefixes.add(prefix_item.get('Prefix').rstrip('/'))
    except Exception as e:
        st.error(f"Error listing prefixes: {e}")
    return sorted(list(prefixes))

@st.cache_data
def fetch_all_json_objects(_client, bucket, keys):
    """
    Fetch and parse all JSON objects from S3 concurrently.

    Args:
        _client: Boto3 S3 client.
        bucket (str): S3 bucket name.
        keys (list): List of S3 object keys.

    Returns:
        List of parsed JSON objects.
    """
    def fetch_json(key):
        try:
            obj = _client.get_object(Bucket=bucket, Key=key)
            json_content = obj['Body'].read().decode('utf-8')
            return json.loads(json_content)
        except Exception as e:
            st.error(f"Error fetching {key}: {e}")
            return None

    json_data = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_json, key): key for key in keys}
        for future in as_completed(futures):
            data = future.result()
            if data:
                json_data.append(data)
    return json_data

def process_json_data(json_objects):
    """
    Process list of JSON objects into a DataFrame.

    Args:
        json_objects (list): List of JSON data.

    Returns:
        pandas.DataFrame: Processed data.
    """
    data_list = []
    for json_data in json_objects:
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
            key_entry = entry.get('key')
            value_dict = entry.get('value', {}).get('Value', {})
            if 'StringValue' in value_dict:
                val = value_dict['StringValue']
                data_list.append({'datetime': dt, 'key': key_entry, 'value': val})
            elif 'LocationValue' in value_dict:
                location = value_dict['LocationValue']
                lat = location.get('latitude')
                lon = location.get('longitude')
                data_list.append({'datetime': dt, 'key': f"{key_entry}_lat", 'value': lat})
                data_list.append({'datetime': dt, 'key': f"{key_entry}_lon", 'value': lon})
            elif 'Invalid' in value_dict and value_dict['Invalid']:
                data_list.append({'datetime': dt, 'key': key_entry, 'value': None})
            else:
                data_list.append({'datetime': dt, 'key': key_entry, 'value': None})
    return pd.DataFrame(data_list)

def main():
    bucket_name = AWS_BUCKET_NAME

    # Sidebar for selections
    with st.sidebar:
        st.header("🔍 Filter Options")
        
        # Get the list of VINs
        prefixes = list_prefixes(s3_client, bucket_name, prefix='', delimiter='/')
        vins = [vin.split('/')[0] for vin in prefixes]  # Extract VINs
        vins = list(sorted(set(vins)))  # Ensure unique and sorted
        if not vins:
            st.error("No VINs found in the bucket. Please check your S3 connection and bucket contents.")
            st.stop()

        selected_vin = st.selectbox('Select VIN', vins)

        if selected_vin:
            # List available years for the selected VIN
            vin_prefix = f"{selected_vin}/"
            years_prefixes = list_prefixes(s3_client, bucket_name, prefix=vin_prefix, delimiter='/')
            years = [year.split('/')[-1] for year in years_prefixes]  # Extract years
            years = list(sorted(set(years)))
            if not years:
                st.warning("No years found for the selected VIN.")
                st.stop()

            selected_year = st.selectbox('Select Year', years)

            if selected_year:
                year_prefix = f"{vin_prefix}{selected_year}/"
                months_prefixes = list_prefixes(s3_client, bucket_name, prefix=year_prefix, delimiter='/')
                months = [month.split('/')[-1] for month in months_prefixes]  # Extract months
                months = list(sorted(set(months)))
                if not months:
                    st.warning("No months found for the selected VIN and year.")
                    st.stop()

                selected_month = st.selectbox('Select Month', months)

                if selected_month:
                    month_prefix = f"{year_prefix}{selected_month}/"
                    days_prefixes = list_prefixes(s3_client, bucket_name, prefix=month_prefix, delimiter='/')
                    days = [day.split('/')[-1] for day in days_prefixes]  # Extract days
                    days = list(sorted(set(days)))
                    if not days:
                        st.warning("No days found for the selected VIN, year, and month.")
                        st.stop()

                    selected_day = st.selectbox('Select Day', days)

                    if selected_day:
                        # List all JSON files under the selected day
                        day_prefix = f"{month_prefix}{selected_day}/"
                        paginator = s3_client.get_paginator('list_objects_v2')
                        json_files = []
                        try:
                            for result in paginator.paginate(Bucket=bucket_name, Prefix=day_prefix):
                                for obj in result.get('Contents', []):
                                    key = obj['Key']
                                    if key.endswith('.json'):
                                        json_files.append(key)
                        except Exception as e:
                            st.error(f"Error listing JSON files: {e}")
                            st.stop()

                        st.markdown(f"**Found {len(json_files)} JSON files for VIN `{selected_vin}` on {selected_year}-{selected_month}-{selected_day}**")

    # Main content area
    st.markdown("---")
    if 'selected_day' in locals() and selected_day:
        if json_files:
            with st.spinner('Fetching and processing JSON files...'):
                json_objects = fetch_all_json_objects(s3_client, bucket_name, json_files)
                df = process_json_data(json_objects)

            if not df.empty:
                # Display raw data
                st.subheader("📊 Raw Data")
                st.dataframe(df)

                # Data Processing
                df['key'] = df['key'].astype(str)
                df['value_numeric'] = pd.to_numeric(df['value'], errors='coerce')

                unique_keys = df['key'].unique()
                selected_keys = st.multiselect('Select Keys to Plot', unique_keys, key='selected_keys')

                if selected_keys:
                    st.subheader("📈 Data Visualizations")

                    # Organize plots in tabs
                    tabs = st.tabs(selected_keys)

                    for tab, key in zip(tabs, selected_keys):
                        with tab:
                            df_key = df[df['key'] == key].copy()
                            df_key.sort_values('datetime', inplace=True)

                            st.markdown(f"### `{key}`")
                            st.dataframe(df_key)

                            if df_key['value_numeric'].notnull().any():
                                fig = px.line(df_key, x='datetime', y='value_numeric', title=f"{key} Over Time")
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning(f"Key `{key}` has non-numeric values or no valid data to plot.")
                                st.dataframe(df_key[['datetime', 'value']])
                else:
                    st.warning("🔔 Please select at least one key to plot.")
            else:
                st.warning("⚠️ No data available to display.")
        else:
            st.warning("⚠️ No JSON files found for the selected day.")
    else:
        st.info("📝 Please use the sidebar to select VIN, Year, Month, and Day to view telemetry data.")

    # Footer
    st.markdown("---")
    st.markdown("© 2024 Fleet Manager. All rights reserved.")

if __name__ == "__main__":
    main()
