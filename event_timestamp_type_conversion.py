#!/usr/bin/env python

"""
Timestamp data in the events stream currently has timestamp information as a
string. Firestore however, has a native timestamp type which should be used
instead. Note that currently, all timestamp information is also in the local
timezone, which would be PST in this case.
"""

import argparse

import firebase_admin
from firebase import firebase
from firebase_admin import credentials, firestore

def _create_firestore_instance(credential_file):
    """
    Creates a new Firestore instance given the parsed command line arguments.

    Args:
        credential_file: A filepath to the credentials file.
    """
    cred = credentials.Certificate(credential_file)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db

parser = argparse.ArgumentParser()
parser.add_argument('--firestore_creds', type=str, help='The path to the credentials file for Firestore', required=True)
args = parser.parse_args()

firestore_db = _create_firestore_instance(args.firestore_creds)