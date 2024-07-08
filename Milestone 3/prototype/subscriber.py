from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPICallError, RetryError
import json
import tkinter as tk
from tkinter import messagebox
from queue import Queue, Empty
import threading

from google.oauth2 import service_account

# Path to your service account key file
key_path = r"nomadic-girder-325214-2364975f70f0.json"

# Explicitly use service account credentials by specifying the private key file path
credentials = service_account.Credentials.from_service_account_file(key_path)

# Configuration
project_id = "nomadic-girder-325214"
subscription_id = "Truck_Data"

subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Initialize Tkinter
root = tk.Tk()
root.title("Cargo Information")
root.geometry("400x400")

# Frame for left-aligned labels
frame = tk.Frame(root)
frame.pack(anchor='w', padx=10, pady=10)

# Labels for displaying information
latitude_label = tk.Label(frame, text="Latitude: ", anchor='w', justify='left')
latitude_label.pack(fill='x')

longitude_label = tk.Label(frame, text="Longitude: ", anchor='w', justify='left')
longitude_label.pack(fill='x')

customer_name_label = tk.Label(frame, text="Customer Name: ", anchor='w', justify='left')
customer_name_label.pack(fill='x')

cargo_type_label = tk.Label(frame, text="Cargo Type: ", anchor='w', justify='left')
cargo_type_label.pack(fill='x')

truck_id_label = tk.Label(frame, text="Truck ID: ", anchor='w', justify='left')
truck_id_label.pack(fill='x')

driver_label = tk.Label(frame, text="Driver: ", anchor='w', justify='left', pady=10)
driver_label.pack(fill='x')

# Labels for disruption information
disruption_label = tk.Label(frame, text="", anchor='w', justify='left', pady=20)
disruption_label.pack(fill='x')

# Queue for thread-safe UI updates
ui_queue = Queue()

def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()  # Acknowledge that the message has been received

    # Process the message
    try:
        data = json.loads(message.data)
        ui_queue.put(data)
    except json.JSONDecodeError:
        print("Error decoding the message data")

def process_cargo_data(data):
    if "disruption_type" in data:
        disruption_type = data.get('disruption_type', "Unknown")
        truck_id = data.get('truck_id', "Unknown")
        disruption_label.config(text=f"Disruption: {disruption_type} for Truck ID: {truck_id}")
        messagebox.showinfo("Disruption Alert", f"Disruption: {disruption_type} for Truck ID: {truck_id}")
        return

    latitude = data.get('latitude', 0)
    longitude = data.get('longitude', 0)
    customer_name = data.get('customer_name', "Unknown")
    cargo_type = data.get('cargo_type', "Unknown")
    truck_id = data.get('truck_id', "Unknown")
    driver = data.get('driver', "Unknown")

    # Update the UI with the received data
    latitude_label.config(text=f"Latitude: {latitude}")
    longitude_label.config(text=f"Longitude: {longitude}")
    customer_name_label.config(text=f"Customer Name: {customer_name}")
    cargo_type_label.config(text=f"Cargo Type: {cargo_type}")
    truck_id_label.config(text=f"Truck ID: {truck_id}")
    driver_label.config(text=f"Driver: {driver}")

    # Show an alert if the cargo is close to the customer location
    customer_location = {"latitude": 52.2152, "longitude": 7.0281}
    car_location = (latitude, longitude)

    if abs(customer_location['latitude'] - car_location[0]) < 0.02 and \
       abs(customer_location['longitude'] - car_location[1]) < 0.02:
        messagebox.showinfo("Alert", f"Truck {truck_id} driven by {driver} is near Customer {customer_name} and will arrive soon")
    else:
        print("Truck is not close to the customer. No action taken.")

def main():
    print("Listening for messages on subscription: ", subscription_path)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()  # Trigger the shutdown
    except GoogleAPICallError as e:
        print(f"API call error: {e}")
    except RetryError as e:
        print(f"Retry error: {e}")

def periodic_check():
    """Check the queue for new data and update the UI."""
    try:
        while True:
            data = ui_queue.get_nowait()
            process_cargo_data(data)
    except Empty:
        pass
    finally:
        root.after(100, periodic_check)

if __name__ == '__main__':
    threading.Thread(target=main, daemon=True).start()
    root.after(100, periodic_check)
    root.mainloop()
