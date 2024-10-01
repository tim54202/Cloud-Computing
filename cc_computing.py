import requests
import decimal
import boto3
import sys

# Define a function to fetch sensor data
def fetch_sensor_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Define a function to convert floats to decimal for DynamoDB compatibility
def float_to_decimal(value):
    if isinstance(value, float):
        return decimal.Decimal(str(value))
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [float_to_decimal(v) for v in value]
    return value

# Calculate AQI
def calculate_aqi(pm25, pm10):
    if pm25 is None and pm10 is None:
        return None

    # Inner function to calculate sub-index for a pollutant
    def calculate_sub_aqi(pm_value, lookup_table):
        for (range_min, range_max), aqi in lookup_table.items():
            if range_min <= pm_value <= range_max:
                return aqi
        return None

    # AQI lookup tables for PM2.5 and PM10
    pm25_lookup_table = {
        (0, 11): 1,
        (12, 23): 2,
        (24, 35): 3,
        (36, 41): 4,
        (42, 47): 5,
        (48, 53): 6,
        (54, 58): 7,
        (59, 64): 8,
        (65, 70): 9,
        (71, float('inf')): 10
    }

    pm10_lookup_table = {
        (0, 16): 1,
        (17, 33): 2,
        (34, 50): 3,
        (51, 58): 4,
        (59, 66): 5,
        (67, 75): 6,
        (76, 83): 7,
        (84, 91): 8,
        (92, 100): 9,
        (101, float('inf')): 10
    }

    # Calculate individual AQI for PM2.5 and PM10
    pm25_aqi = calculate_sub_aqi(pm25, pm25_lookup_table)
    pm10_aqi = calculate_sub_aqi(pm10, pm10_lookup_table)

    # Return the higher of the two AQI values
    if pm25_aqi is not None and pm10_aqi is not None:
        return max(pm25_aqi, pm10_aqi)
    elif pm25_aqi is not None:
        return pm25_aqi
    elif pm10_aqi is not None:
        return pm10_aqi
    else:
        return None

# Specify the fields to keep for DynamoDB
fields_to_keep = ['id', 'timestamp', 'country', 'sensordatavalues']

# Fetch JSON data using fetch_sensor_data
data = fetch_sensor_data('https://data.sensor.community/static/v2/data.json')

# Process and transform the data, including conversion to decimal
for entry in data:
    entry['sensordatavalues'] = float_to_decimal(entry['sensordatavalues'])

# Use personal account's region name and database table
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('test1')

# Define the batch size
batch_size = 25
batch_items = []

# Use a batch writer to handle batch operations
with table.batch_writer(overwrite_by_pkeys=['id']) as batch:  # Add overwrite_by_pkeys to prevent duplicates
    for i, entry in enumerate(data, 1):
        sensordatavalues = entry['sensordatavalues']
        pm10 = None
        pm25 = None
        for value in sensordatavalues:
            if value['value_type'] == 'P1':
                pm10 = decimal.Decimal(value['value'])
            elif value['value_type'] == 'P2':
                pm25 = decimal.Decimal(value['value'])
        # Calculate AQI
        if pm10 is not None and pm25 is not None:
            aqi = calculate_aqi(pm10, pm25)
            entry['AQI'] = aqi

        # Create a new dictionary containing only the fields to keep
        item_to_insert = {
            'id': str(entry['id']),  # Convert number to string for DynamoDB
            'timestamp': entry['timestamp'],
            'country': entry['location']['country'],
            'pm10': pm10,
            'pm25': pm25,
            'AQI': aqi
        }

        # Add the item to the batch list
        batch_items.append(item_to_insert)

        # Perform batch write when the batch list reaches the specified size
        if len(batch_items) == batch_size:
            for item in batch_items:
                batch.put_item(Item=item)
            # Clear the batch list after the write
            batch_items = []

    # Process any remaining items in the batch list
    for item in batch_items:
        batch.put_item(Item=item)


# Check if command line arguments were provided
if len(sys.argv) > 1:
    command = sys.argv[1]  # Get the command from the command line arguments
    if command == 'update-data':
        ssm_client = boto3.client('ssm', region_name='us-east-1')
        instance_id = 'i-0db26dbbac2924a3c'
        document_name = 'runScript'

        # Send the command to the specified EC2 instance using SSM Run Command
        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName=document_name,
            Parameters={},  # Add any necessary parameters for the SSM document
            Comment='Data update command'  # A comment to describe the purpose of this command
        )
        print('SSM command has been sent:', response)
    else:
        print('No valid command provided.')
else:
    print('No command line argument provided.')
