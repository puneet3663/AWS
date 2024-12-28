import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    # Initialize the CloudWatch client
    cloudwatch = boto3.client('cloudwatch')
    
    # Define the time frame (last 5 minutes)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=5)
    
    # Query CloudWatch for API Gateway metrics
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/ApiGateway',  # Namespace for API Gateway metrics
        MetricName='Count',          # Metric for total API requests
        Dimensions=[
            {
                'Name': 'ApiName',   # Replace with your API Gateway's name
                'Value': 'YourApiName'
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=300,                  # Period in seconds (5 minutes)
        Statistics=['Sum']           # Sum provides the total number of requests
    )
    
    # Extract the total number of API calls
    datapoints = response.get('Datapoints', [])
    total_api_calls = sum(dp['Sum'] for dp in datapoints) if datapoints else 0
    
    # Log and return the total API calls
    print(f"Total API calls in the last 5 minutes: {total_api_calls}")
    return {
        'statusCode': 200,
        'body': f"Total API calls in the last 5 minutes: {total_api_calls}"
    }
