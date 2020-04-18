#!/usr/bin/env python

"""
Create a google cloud auth token for firestore. Since the jwt has to go through
various processes to get the actual end result, this python script will merely
handle the creation for us. This is an easy then to also test the raw REST API
which the ESP32 uses.
"""

import argparse
import base64
import json
import jwt
import requests
import time

parser = argparse.ArgumentParser()
parser.add_argument('--credentials', help='The path to the credentials file for the service account')
args = parser.parse_args()

ALG = 'RS256'
SCOPE = 'https://www.googleapis.com/auth/datastore'

with open(args.credentials, 'r') as f:
    credential_def = json.load(f)

timestamp = int(time.time())
later = timestamp + 3599
payload = {
    'iss': credential_def['client_email'],
    'scope': SCOPE,
    'aud': credential_def['token_uri'],
    'iat': timestamp,
    'exp': later
}
token = jwt.encode(payload, credential_def['private_key'], algorithm=ALG)

auth_url = 'https://oauth2.googleapis.com/token?grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=' + token.decode('utf-8')
resp = requests.post(auth_url)
resp.raise_for_status()
j = resp.json()
print(j['access_token'])
