#!/usr/bin/env python

"""
This is the main driver code for wrapping management of the Firestore instance.
Through this driver, accounts, locations, and streams can be updated. In the
future this may be a more advanced driver with a GUI, and more advanced
functionality for certain common functionality.

Current functionality will be:
    - Create a new account. This corresponds to a business or organization.
    - Add a new location to an account.
    - Create a new stream for a location on an account. This will be the case if
        any location has more than one surveybox (eg. at more than one register)

For the moment this will only be focused on adding new information easily, since
the web interface is not very easy for this, but it is very easy to delete
information.
"""

import argparse

import firebase_admin
from firebase_admin import credentials, firestore

def _create_db_instance(args):
    """
    Creates a new db instance given the parsed command line arguments.

    Args:
        args: The parsed command line arguments.
    """
    cred = credentials.Certificate(args.credentials)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db


def add_account(args):
    """
    The handler for the add-account command. Adds a new account.

    Args:
        args: The parsed command line object.
    """
    db = _create_db_instance(args)

    payload = {
        'name': args.name
    }
    doc_ref = db.collection('accounts').document()
    doc_ref.create(payload)
    print(doc_ref.id)


def add_location(args):
    """
    The handler for the add-location command. Adds a new location for an account

    Args:
        args: The parsed command line object.
    """
    db = _create_db_instance(args)

    payload = {
        'name': args.name,
        'coords': firestore.GeoPoint(args.lat, args.lon)
    }

    account_doc = db.document('accounts', args.account_id)
    locations_col = account_doc.collection('locations')
    location_doc_ref = locations_col.document()
    location_doc_ref.create(payload)
    print(location_doc_ref.id)


def add_stream(args):
    """
    The handler for the add-stream command. Adds a new stream to a location.

    Args:
        args: The parsed command line object.
    """
    db = _create_db_instance(args)

    loc_ref = db.document('accounts', args.account_id, 'locations', args.location_id)
    payload = {
        'name': args.name,
        'location': loc_ref
    }
    stream_doc_ref = db.collection('streams').document()
    stream_doc_ref.create(payload)
    print(stream_doc_ref.id)



parser = argparse.ArgumentParser()
parser.add_argument('--project', type=str, help='The name of the Firestore project to connect to.', required=True)
parser.add_argument('--credentials', type=str, help='The location of the credentials file', required=True)
subparsers = parser.add_subparsers(help='The sub-commands on this manager driver')

# Parser for 'add-account' command.
add_account_parser = subparsers.add_parser('add-account', help='Creates a new account instance.')
add_account_parser.add_argument('--name', type=str, help='The name of the account (eg. Cafe Allegro)')
add_account_parser.set_defaults(func=add_account)

# Parser for 'add-location' command.
add_location_parser = subparsers.add_parser('add-location', help='Creates a new location under an existing account')
add_location_parser.add_argument('--account_id', type=str, help='The name of the account to create the location under', required=True)
add_location_parser.add_argument('--name', type=str, help='A human readable name associated with the location', required=True)
add_location_parser.add_argument('--lat', type=float, help='The latitude of the new location.', required=True)
add_location_parser.add_argument('--lon', type=float, help='The longitude of the new location.', required=True)
add_location_parser.set_defaults(func=add_location)

# Parser for 'add-stream' command.
add_stream_parser = subparsers.add_parser('add-stream', help='Creates a new stream associated with a specific location')
add_stream_parser.add_argument('--account_id', type=str, help='The id of the account for the new stream', required=True)
add_stream_parser.add_argument('--location_id', type=str, help='The id of the location for the new stream', required=True)
add_stream_parser.add_argument('--name', type=str, help='The name of the new stream', required=True)
add_stream_parser.set_defaults(func=add_stream)

args = parser.parse_args()
args.func(args)
