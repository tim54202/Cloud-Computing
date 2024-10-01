import boto3

def lambda_handler(event, context):
    # Check if the 'command' key is present in the event object and if it equals 'update-data'
    if event.get('command') == 'update-data':
        ssm_client = boto3.client('ssm', region_name='us-east-1')

        # Specify the EC2 instance ID and the name of the SSM document
        instance_id = 'i-0db26dbbac2924a3c'
        document_name = 'runScript'

        # Send the SSM command to the specified EC2 instance to perform data update
        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName=document_name,
            Comment='Run my custom script',
            Parameters={}
        )

        return {
            'statusCode': 200,
            'body': 'SSM command sent to EC2 instance to update data'
        }
    else:
        # If 'update-data' command is not found, return an error message
        return {
            'statusCode': 400,
            'body': 'No valid update command found in the event'
        }

