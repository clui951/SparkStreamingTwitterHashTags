import argparse
import json
import requests
import requests_oauthlib
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
    global ACCESS_TOKEN
    global ACCESS_SECRET
    global CONSUMER_KEY
    global CONSUMER_SECRET

    ACCESS_TOKEN = args['accesstoken']
    ACCESS_SECRET = args['accesssecret']
    CONSUMER_KEY = args['consumerkey']
    CONSUMER_SECRET = args['consumersecret']

    if 'tcpip' in args:
        global TCP_IP
        TCP_IP = args['tcpip']
    if 'tcpport' in args:
        global TCP_PORT
        TCP_PORT = args['tcpport']


def get_tweets_stream_response(auth):
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=auth, stream=True)
	print(query_url, response)
	return response

def read_tweets_stream_response(stream_response):
    for line in stream_response.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print("------------------------------------------")
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


def main():
    parse_global_args()
    auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    stream_response = get_tweets_stream_response(auth)
    read_tweets_stream_response(stream_response)

if __name__ == '__main__':
    main()