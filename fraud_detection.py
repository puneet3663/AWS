import boto3
import time

# Initialize DynamoDB and SNS clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

TRANSACTIONS_TABLE = 'Transactions'
TOPIC_ARN = 'arn:aws:sns:region:account-id:alert-topic'

def lambda_handler(event, context):
    # Parse transaction details from event
    transaction = event['detail']
    card_id = transaction['card_id']
    timestamp = transaction['timestamp']
    
    table = dynamodb.Table(TRANSACTIONS_TABLE)
    
    # Fetch recent transactions for this card
    response = table.get_item(Key={'card_id': card_id})
    item = response.get('Item', {'transactions': []})
    
    # Filter transactions within the past 60 seconds
    current_time = int(time.time())
    recent_transactions = [
        t for t in item['transactions'] if current_time - t['timestamp'] <= 60
    ]
    recent_transactions.append({'timestamp': timestamp})
    
    # Update transactions in DynamoDB
    table.put_item(
        Item={
            'card_id': card_id,
            'transactions': recent_transactions
        }
    )
    
    # Trigger alert if more than 2 transactions
    if len(recent_transactions) > 2:
        sns.publish(
            TopicArn=TOPIC_ARN,
            Message=f"Alert: More than 2 transactions on card {card_id} within 1 minute.",
            Subject='Suspicious Credit Card Activity'
        )
