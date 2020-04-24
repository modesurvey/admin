#!/usr/bin/env python

"""
Timestamp data in the events stream currently has timestamp information as a
string. Firestore however, has a native timestamp type which should be used
instead. Note that currently, all timestamp information is also in the local
timezone, which would be PST in this case.
"""

import argparse
import datetime

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
parser.add_argument('--credentials', type=str, help='The path to the credentials file for Firestore', required=True)
parser.add_argument('--stream_ids', nargs='+', help='The stream ids to convert.', required=True)
args = parser.parse_args()

firestore_db = _create_firestore_instance(args.credentials)

for stream_id in args.stream_ids:
    events = firestore_db.collection('streams', stream_id, 'events')
    for event_ref in events.list_documents():
        event_doc = event_ref.get()
        try:
            timestamp = event_doc.get('timestamp')
            if isinstance(timestamp, str):
                timestamp_f = float(timestamp)
                timestamp_obj = datetime.datetime.utcfromtimestamp(timestamp_f)
                event_ref.update({'timestamp': timestamp_obj})
                print('Transformed {} with value {} to {}'.format(event_ref.path, timestamp, timestamp_obj))
            else:
                print('Field timestamp on {} is not a str.'.format(event_ref.path))
        except KeyError:
            print('Event with id {} did not have a timestamp field.'.format(event_ref.path))
