from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import argparse
import logging
import sys
import requests

TCP_IP = None
TCP_PORT = None

logger = None

def parse_global_args():
    parser = argparse.ArgumentParser(description='Twitter client that calls Twitter API ' +
                                                 'and returns response for a stream of tweets')

    # TCP Configuration
    parser.add_argument('--tcpip', help='Twitter client IP', required=False)
    parser.add_argument('--tcpport', help='Twitter client port', required=False)

    # debug logging
    parser.add_argument('--debug', action='store_true')

    # set global args
    args = vars(parser.parse_args())
    global TCP_IP
    global TCP_PORT

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
        logging.basicConfig(level=logging.ERROR)
    global logger
    logger = logging.getLogger("SparkClient")


def aggregate_tags_state_count(new_values, prev_value):
    return sum(new_values) + (prev_value or 0)


def get_sql_context_instance(spark_context):
    # create SQLContext from spark_context if doesn't already exist; return SQLContext
    if ('sqlContextSingleInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]

    # extract the counts from dataframe and convert them into array
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]

    # initialize and send the data through REST API
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # get spark sql singleton context from current context
        sql_context = get_sql_context_instance(rdd.context)

        # convert RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))

        # create dataframe from Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)

        # register the dataframe as table
        hashtags_df.registerTempTable("hashtags")

        # query top 10 hashtags from the table using SQL and print them
        hashtag_top_counts_df = sql_context.sql("SELECT hashtag, hashtag_count FROM hashtags ORDER BY hashtag_count DESC LIMIT 10")
        hashtag_top_counts_df.show()

        # call this method to send hashtag_top_counts_df to dashboard
        send_df_to_dashboard(hashtag_top_counts_df)

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def main():
    parse_global_args()

    # create spark configuration
    conf = SparkConf()
    conf.setAppName("SparkTwitterHashTags")

    # create spark context with above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # create streaming context from above spark context, with interval size 2 seconds
    ssc = StreamingContext(sc, 2)

    # set checkpoint to allow RDD recovery; for stateful transformations beyond the microbatches
    ssc.checkpoint("checkpoint_SparkTwitterHashTags")

    # read data from port
    dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

    # split each tweet into words
    words = dataStream.flatMap(lambda line: line.split(" "))

    # filter words to only hashtags, then map each hashtag to a pair of (hashtag, 1)
    hashtags_tuple = words.filter(lambda word: word.startswith('#')).map(lambda hashtag: (hashtag, 1))

    # add the count of each hashtag to its last count
    tags_totals = hashtags_tuple.updateStateByKey(aggregate_tags_state_count)

    # do processing for each RDD generated in each interval (single rdd contains all counts at current time)
    # and send to external system
    tags_totals.foreachRDD(process_rdd)


    # start streaming computation
    ssc.start()

    # wait for streaming to finish
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
