import argparse
import json
import logging
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

logger = None

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

    # debug logging
    parser.add_argument('--debug', action='store_true')

    # set global args
    args = vars(parser.parse_args())
    global ACCESS_TOKEN
    global ACCESS_SECRET
    global CONSUMER_KEY
    global CONSUMER_SECRET
    global TCP_IP
    global TCP_PORT

    ACCESS_TOKEN = args['accesstoken']
    ACCESS_SECRET = args['accesssecret']
    CONSUMER_KEY = args['consumerkey']
    CONSUMER_SECRET = args['consumersecret']

    if args['tcpip']:
        TCP_IP = args['tcpip']
    else:
        TCP_IP = 'localhost'
    if args['tcpport']:
        TCP_PORT = args['tcpport']
    else:
        TCP_PORT = 9009

    setup_logger(args['debug'])


def setup_logger(debug = False):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    global logger
    logger = logging.getLogger("TwitterClient")


def get_spark_connection():
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.info(TCP_IP)
    logger.info(TCP_PORT)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    logger.info("Waiting for TCP connection to spark...")
    conn, addr = s.accept()
    logger.info("Connected to spark.")
    return conn


def get_tweets_stream_response(auth):
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=auth, stream=True)
    logger.info(query_url + response.__str__())
    return response


def send_tweets_stream_response_to_spark(stream_response, conn):
    for line in stream_response.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            logger.info("Tweet Text: " + tweet_text)
            logger.info("------------------------------------------")
            conn.send(tweet_text + "\n")
        except:
            e = sys.exc_info()[0]
            logger.error("Error: %s" % e)


def main():
    parse_global_args()
    auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    conn = get_spark_connection()
    tweets_stream_response = get_tweets_stream_response(auth)
    send_tweets_stream_response_to_spark(tweets_stream_response, conn)

if __name__ == '__main__':
    main()