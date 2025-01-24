import boto3
import time
import json

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name='your-region')

# Define your Kinesis Data Stream name
STREAM_NAME = 'TransactionStream'

def get_shard_iterator(stream_name):
    """Get the initial shard iterator for the Kinesis stream."""
    response = kinesis_client.describe_stream(StreamName=stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    # Get the shard iterator
    iterator_response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='LATEST'  # Start reading from the latest records
    )
    return iterator_response['ShardIterator']

def read_from_stream(stream_name):
    """Continuously read and print data from the Kinesis stream."""
    shard_iterator = get_shard_iterator(stream_name)

    print(f"Reading data from stream: {stream_name}\n")
    
    while True:
        # Get records from the shard
        response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
        records = response['Records']
        
        # Print the records to the terminal
        for record in records:
            data = json.loads(record['Data'])
            print(f"Received Record: {data}")
        
        # Update the shard iterator to fetch the next batch of records
        shard_iterator = response['NextShardIterator']
        
        # Sleep to avoid exceeding API call rate limits
        time.sleep(1)

if __name__ == "__main__":
    try:
        read_from_stream(STREAM_NAME)
    except KeyboardInterrupt:
        print("\nStopped reading from Kinesis stream.")
