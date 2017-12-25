import argparse
import json
import requests
import requests_oauthlib
import socket
import sys

ACCESS_TOKEN = 'ACCESS_TOKEN'
ACCESS_SECRET = 'ACCESS_SECRET'
CONSUMER_KEY = 'CONSUMER_KEY'
CONSUMER_SECRET = 'CONSUMER_SECRET'

TCP_IP = 'localhost'
TCP_PORT = 9009


def parse_global_args():
    parser = argparse.ArgumentParser(description='Twitter client that calls Twitter API ' +
                                                 'and returns response for a stream of tweets')

    # Twitter API
    parser.add_argument('--accesstoken', help='Twitter API access token', required=True)
    parser.add_argument('--accesssecret', help='Twitter API access secret', required=True)
    parser.add_argument('--consumerkey', help='Twitter API consumer key', required=True)
    parser.add_argument('--consumersecret', help='Twitter API consumer secret', required=True)

    # TCP Configuration
    parser.add_argument('--tcpip', help='Twitter client IP', required=False)
    parser.add_argument('--tcpport', help='Twitter client port', required=False)

    # set global args
    args = vars(parser.parse_args())
    ACCESS_TOKEN = args['accesstoken']
    ACCESS_SECRET = args['accesssecret']
    CONSUMER_KEY = args['consumerkey']
    CONSUMER_SECRET = args['consumersecret']

    if 'tcpip' in args:
        TCP_IP = args['tcpip']
    if 'tcpport' in args:
        TCP_PORT = args['tcpport']


def main():
    parse_global_args()
    my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    print("Done! :)")


if __name__ == '__main__':
    main()