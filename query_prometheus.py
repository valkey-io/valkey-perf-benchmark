#!/usr/bin/env python3
"""Query metrics from AWS Managed Prometheus."""
import json

import boto3
import requests
from requests_aws4auth import AWS4Auth

workspace_url = "https://aps-workspaces.us-east-1.amazonaws.com/workspaces/ws-624b5b5f-f73f-43bb-ac0c-0b8e2829525f"
region = "us-east-1"

session = boto3.Session()
credentials = session.get_credentials()
auth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    "aps",
    session_token=credentials.token,
)

# Query all metrics
url = f"{workspace_url}/api/v1/label/__name__/values"
response = requests.get(url, auth=auth)
print(f"All metrics: {response.json()}")

# Query valkey_rps with time range
url = f"{workspace_url}/api/v1/query"
params = {"query": "valkey_rps[1h]"}
response = requests.get(url, params=params, auth=auth)
print(f"\nvalkey_rps query: {response.json()}")
