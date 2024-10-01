import boto3
import json

# Initialize DynamoDB resource and specify the table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('test1')

def lambda_handler(event, context):
    # Retrieve country and timestamp from the query string parameters
    country = event['queryStringParameters']['country']
    timestamp = event['queryStringParameters']['timestamp']

    try:
        # Attempt to retrieve the AQI data from DynamoDB using the provided country and timestamp
        response = table.get_item(
            Key={
                'country': country,
                'timestamp': timestamp
            }
        )

        # Check if the item exists in the response
        if 'Item' in response:
            aqi = response['Item']['aqi']
            # Return a successful response with the AQI data
            return {
                'statusCode': 200,
                'body': json.dumps({'aqi': aqi})
            }
        else:
            # If the item is not found, return a 404 response
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'AQI data not found'})
            }
    except Exception as e:
        # Handle any exceptions that occur and return a 500 response
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
