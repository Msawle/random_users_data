import requests
import csv
import pandas as pd
from google.cloud import storage
import os
import gcsfs


def get_random_users(requests):
    """
    Cloud Function to fetch data for 1000 random users and save it to a CSV file.

    Args:
        requests: The request object.

    Returns:
        A JSON response indicating success or failure.
    """

    filename = "input/random_users_data.csv"
    bucket_name = "new-gcs-bucket-1989"

    try:
        # Fetch data for 100 users from the Random User API
        response = requests.get("https://randomuser.me/api/?results=1000&nat=gb,us&format=csv&dl")
        # response.raise_for_status()
        user_data = response.text

        # Fetch column names from the response text
        columns = user_data.split("\n")[0].replace(".", "_").split(",")
        records = user_data.split("\n")[1:]

        data = []
        reader = csv.reader(records)

        for row in reader:
            data.append(row)

        _count = save_to_csv(bucket_name, filename, data, columns=columns)
        print(f"User data for {_count} users saved to CSV successfully")

    except requests.exceptions.RequestException as e:
        print(f"Exception: {e}")


def save_to_csv(bucket_name, filename, data, columns=None):
    """
    Save multiple rows of data to a CSV file in Google Cloud Storage.

    Args:
        bucket_name (str): The name of the GCS bucket.
        filename (str): The name of the CSV file.
        data (list): The list of data rows to append to the CSV file.
        columns(list): list of columns
    """
    # Initialize the Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    # Check if the file already exists
    if blob.exists():

        existing_file_path = f"gs://{bucket_name}/{filename}"
        existing_data = pd.read_csv(existing_file_path)
        columns = existing_data.columns
        new_data = pd.DataFrame(data=data, columns=columns)

        # Append the new rows
        merged_df = pd.concat([existing_data, new_data])
        merged_df.to_csv("/tmp/temp.csv", index=None)

        # Upload the updated file back to GCS
        blob.upload_from_filename("/tmp/temp.csv")

        return merged_df.shape[0]

        
    else:
        # Create a new CSV file with headers

        new_data = pd.DataFrame(data=data, columns=columns)
        new_data.to_csv("temp.csv", index=None)
        new_data.shape[0]
        # Upload the new file to GCS
        blob.upload_from_filename("/tmp/temp.csv")

        return new_data.shape[0]

get_random_users(requests)
