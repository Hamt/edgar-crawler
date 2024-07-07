import os
import json
import requests

# Define the directory containing the JSON files
directory = './datasets/EXTRACTED_FILINGS'

# Define the API endpoints and headers
base_url = 'https://nocodb.dev.roe.kr/api/v2/tables/mg2ivk4tww3mnwp/records'
headers = {
    'accept': 'application/json',
    'xc-token': 'h4M9v6NJasRN5aAzqAm3SQorbKZiqPV7-1guUQw-',
    'Content-Type': 'application/json'
}

# Function to check if a record exists
def record_exists(cik, period_of_report):
    where_clause = f"(cik,eq,{cik})~and(period_of_report,eq,exactDate,{period_of_report})"
    response = requests.get(f"{base_url}?where={where_clause}", headers=headers)
    # print(f"{base_url}?where={where_clause}")
    if response.status_code == 200:
        records = response.json().get('list', [])
        if records:
            return records[0]['id']
    return None

# Function to send data to the API
def send_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        cik = data.get('cik')
        period_of_report = data.get('period_of_report')
        if cik and period_of_report:
            record_id = record_exists(cik, period_of_report)
            if record_id:
                # Record exists, update it using PATCH
                data['id'] = record_id
                response = requests.patch(f"{base_url}", headers=headers, json=data)
                if response.status_code == 200:
                    print(f'Successfully updated data from {file_path}')
                else:
                    print(f"Fail URL : {base_url}")
                    print(f'Failed to update data from {file_path}. Status code: {response.status_code}')
            else:
                # Record does not exist, create it using POST
                response = requests.post(base_url, headers=headers, json=data)
                if response.status_code == 200:
                    print(f'Successfully created data from {file_path}')
                else:
                    print(f'Failed to create data from {file_path}. Status code: {response.status_code}')
        else:
            print(f'CIK or period_of_report not found in {file_path}')

# Get the list of JSON files in the directory
json_files = [f for f in os.listdir(directory) if f.endswith('.json')]

# Process each file
for json_file in json_files:
    file_path = os.path.join(directory, json_file)
    send_data(file_path)
