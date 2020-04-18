#!/usr/bin/env python 

"""
The original data structure of surveybox on Firebase was to have two tables. One
for production deployment events, and one for testing events. This quickly
became unsustainable and a better schema had to be created. Firebase design is
still relatively new to the modesurvey team, so the engineering decisions made
here for schema version 1.0.0 may later be changed, but at this stage
improvement is more important than designing the perfect schema.

The new schema will follow the given hierarchy:
    account -> (id, locations, name)
    location -> (id, latitude, longitude)
    stream_info -> (id, display_name, location_id)
    stream_events -> (id, events)
    event -> (timestamp, response)

Note that with Firebase, the id will be implicit in the list structure.
This will allow flexibility in querying and data changes (although it may not be
the most efficient noSQL approach). This migration will also move from Firebase
over to Firestore which seems to be a more promising long term solution related
to data management, and data consistency.
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


class DeploymentTransform:
    def __init__(self, start_id, end_id, stream_id):
        self.start_id = start_id
        self.end_id = end_id
        self.stream_id = stream_id


DEPLOYMENT_TRANSFORMS = [
    DeploymentTransform('-LyQCWa5VCOOEb_smtsm', '-LyQU8kFWy2nLFulL1R3', 'hGVidd0GtFF3Tki3yBf9'),
    DeploymentTransform('-LzwP2eY84X2yDkFUIAK', '-LzyJ3uXgWxaPZxByLPg', 'Kwi4xoIpmsmbcTQDjTr7'),
    DeploymentTransform('-M-6XYd6K1wBT2MCEk9i', '-M-6x15rORxoFH_14qfu', 'hGVidd0GtFF3Tki3yBf9'),
    DeploymentTransform('-M-6UDSkxq5YhZz7Beul', '-M-C-UMnre028IsLYLDH', 'Kwi4xoIpmsmbcTQDjTr7'),
    DeploymentTransform('-M-Fm0ecZEQMgx5Fz0Cj', '-M-GK0iBJZJzBBY0t51H', 'Kwi4xoIpmsmbcTQDjTr7'),
    DeploymentTransform('-M-LH26xNw5P4Xyko7cn', '-M-LcVZPCvU5ayr9-0GO', 'Kwi4xoIpmsmbcTQDjTr7'),
    DeploymentTransform('-M-Q5lQtu2twXoZH_sUj', '-M-QdSGqxIY84wBqkwtM', 'Kwi4xoIpmsmbcTQDjTr7'),
    DeploymentTransform('-M-fG2mOkLIWLvF-Bk0j', '-M-fufudCc5MwTG8z8Dq', 'hGVidd0GtFF3Tki3yBf9')
]


parser = argparse.ArgumentParser()
parser.add_argument('--firebase_project', type=str, help='The name of the Firebase project to connect to.', required=True)
parser.add_argument('--firestore_creds', type=str, help='The path to the credentials file for Firestore', required=True)
args = parser.parse_args()


firebase_uri = 'https://{}.firebaseio.com/'.format(args.firebase_project)
firebase_db = firebase.FirebaseApplication(firebase_uri, None)
firestore_db = _create_firestore_instance(args.firestore_creds)

events_prod = firebase_db.get('/events', None)
events_test = firebase_db.get('/events-test', None)
events = {**events_prod, **events_test}

sorted_events = sorted(events.items(), key=lambda kv: kv[1]['timestamp'])
event_id_to_sorted_index = {}
for i, (event_id, _) in enumerate(sorted_events):
    event_id_to_sorted_index[event_id] = i
sorted_events = list(map(lambda kv: kv[1], sorted_events))

for deployment_transform in DEPLOYMENT_TRANSFORMS:
    # This is the start and end of the ids of the data that was seen during deployment.
    # Since these are the two endpoints, every data point between these two points are
    # valid. Technically, if there is valid data with the same timestamp we may miss
    # this data, but since any data that has the same timestamp as any other data is
    # not valid and is duplicate we can technically get away with this.
    start_index = event_id_to_sorted_index[deployment_transform.start_id]
    end_index = event_id_to_sorted_index[deployment_transform.end_id]
    stream = firestore_db.document('streams', deployment_transform.stream_id)
    events = stream.collection('events')

    while start_index > 0 and sorted_events[start_index - 1]['timestamp'] == sorted_events[start_index]['timestamp']:
        start_index -= 1

    while end_index < (len(sorted_events) - 1) and sorted_events[end_index + 1]['timestamp'] == sorted_events[end_index]['timestamp']:
        end_index += 1

    simplified_events = map(lambda ev: { 'type': ev['type'], 'timestamp': ev['timestamp'] },
            sorted_events[start_index:end_index + 1])
    for event in simplified_events:
        event_doc_ref = events.document()
        event_doc_ref.create(event)
