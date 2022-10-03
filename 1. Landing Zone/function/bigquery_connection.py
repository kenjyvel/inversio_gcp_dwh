# -*- coding: utf-8 -*-
from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "keys/key.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

def bq_conn():
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    return client

