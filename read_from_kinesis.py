import json
import base64

def lambda_handler(event, context):
    """
    Lambda function to process Kinesis Firehose events and print the records.
    """
    try:
        # Iterate through the records in the event
        for record in event['records']:
            # Decode the Base64 data
            decoded_data = base64.b64decode(record['data']).decode('utf-8')
            
            # Parse the JSON payload
            parsed_data = json.loads(decoded_data)
            
            # Print the parsed data
            if parsed_data.get('PRICE', 0) > 150 and parsed_data.get('TICKER_SYMBOL')="BNM":
                print(f"Record: {parsed_data}")
        
        # Return the processed records with "OK" status
        return {
            "records": [
                {
                    "recordId": record["recordId"],
                    "result": "Ok",
                    "data": record["data"]
                } for record in event['records']
            ]
        }
    except Exception as e:
        print(f"Error processing records: {e}")
        raise

