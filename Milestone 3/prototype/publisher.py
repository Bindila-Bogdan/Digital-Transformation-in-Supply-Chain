from google.cloud import pubsub_v1
import json
import time
import random
from google.oauth2 import service_account

# Path to your service account key file
key_path = r"nomadic-girder-325214-2364975f70f0.json"

# Explicitly use service account credentials by specifying the private key file path
credentials = service_account.Credentials.from_service_account_file(key_path)

# Initialize the Pub/Sub client with explicit credentials
publisher = pubsub_v1.PublisherClient(credentials=credentials)

# Configuration
project_id = "nomadic-girder-325214"
topic_id = "TMS_Prototype"

# Initialize the Pub/Sub client with explicit credentials
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project_id, topic_id)

def generate_mock_data():
    """Generates mock data for cargo including location, customer name, type of cargo, truck ID, and driver."""
    min_latitude, max_latitude = 52.2100, 52.2300  # small range around Enschede and Gronau
    min_longitude, max_longitude = 6.8800, 7.0400  # small range around Enschede and Gronau
    latitude = round(random.uniform(min_latitude, max_latitude), 6)
    longitude = round(random.uniform(min_longitude, max_longitude), 6)
    customer_names = ["John Doe", "Jane Smith", "Acme Corp", "Global Shipping"]
    cargo_types = ["Electronics", "Furniture", "Clothing", "Food"]
    truck_ids = ["Truck-123", "Truck-456", "Truck-789", "Truck-101"]
    drivers = ["Alice", "Bob", "Charlie", "Dave"]
    customer_name = random.choice(customer_names)
    cargo_type = random.choice(cargo_types)
    truck_id = random.choice(truck_ids)
    driver = random.choice(drivers)
    return {
        "latitude": latitude,
        "longitude": longitude,
        "customer_name": customer_name,
        "cargo_type": cargo_type,
        "truck_id": truck_id,
        "driver": driver,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def generate_disruption_data():
    """Generates mock disruption data including cooling issues, temperature increase, and road blockages."""
    disruption_types = ["Cooling issue", "Temperature going up", "Road is blocked"]
    disruption_type = random.choice(disruption_types)
    truck_ids = ["Truck-123", "Truck-456", "Truck-789", "Truck-101"]
    truck_id = random.choice(truck_ids)
    return {
        "disruption_type": disruption_type,
        "truck_id": truck_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def publish_message(data):
    """Publishes a message to a specified Pub/Sub topic."""
    message_json = json.dumps(data)
    message_bytes = message_json.encode('utf-8')
    try:
        publish_future = publisher.publish(topic_path, message_bytes)
        publish_future.result()  # Verifies the publish succeeded
        print(f'Message published: {message_json}')
    except Exception as e:
        print(f'An error occurred: {e}')

def publish_periodically():
    """Publishes messages every 5 seconds, alternating between cargo data and disruption data every 30 seconds."""
    counter = 0
    while True:
        if counter % 6 == 0:  # Every 30 seconds (6 times 5 seconds)
            data = generate_disruption_data()
        else:
            data = generate_mock_data()
        publish_message(data)
        counter += 1
        time.sleep(5)  # Wait for 5 seconds before next publish

if __name__ == '__main__':
    publish_periodically()
