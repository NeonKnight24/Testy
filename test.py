import json
import requests
from random import randrange

# Demo script that shows how data can be ingested through rabbitmq.
headers = {}

# Read metadata.json and use its contents as the header
# IMPORTANT uuid needs to be changed everytime data is ingested as system does not allow for duplicate data
with open('metadatatest.json', 'r') as f:
    headers["metadata"] = json.load(f)

headers['metadata']['descriptor']['uuid'] = headers['metadata']['descriptor']['uuid'] + str(randrange(100000))
headers['metadata'] = json.dumps(headers['metadata'])

# Add the additional header
headers['amqp$vhost'] = 'vAmikom'
headers['amqp$exchange'] = 'geojson'
headers['amqp$routingKey'] = 'output.df_data'
headers['amqp$deliveryMode'] = '1'
# Read data.geojson and use its contents as the data to be posted
file = './v2_31jan_complete_ndvi_2015.json'
with open(file, 'rb') as f:
    data = f.read()

# URL to post data
url = 'http://10.20.20.3:30516/metadata/ingest'

# Post the data with the metadata as the header
response = requests.post(url, headers=headers, data=data)

# Print the response
print(response.text)

# Optionally, handle the response. For example, check if the request was successful:
if response.status_code == 200:
    print("Data successfully posted!")
else:
    print(f"Failed to post data. Status code: {response.status_code}. Response text: {response.text}")
