import datetime
import time        # For adding delays and generating timestamps
import random      # For generating random data for simulation
import json        # For serializing the event data into JSON format
from azure.eventhub import EventHubProducerClient, EventData # Specifically for interacting with Azure Event Hubs

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://ecommrealtime-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=8nwQMWpD/vsN99KbpblKyHQDtO1vFt6qP+AEhNiYxpI="
EVENT_HUB_NAME = "ecommerce-data-stream"

users = [f"user_{i}" for i in range(1, 501)] 
products = [
    {"id": "PROD_EL_SMART_S23", "name": "Samsung Galaxy S23", "category": "electronics", "brand": "Samsung"},
    {"id": "PROD_EL_IPHONE_15", "name": "Apple iPhone 15", "category": "electronics", "brand": "Apple"},
    {"id": "PROD_CL_TSHIRT_NK", "name": "Nike Sportswear T-Shirt", "category": "clothing", "brand": "Nike"},
    {"id": "PROD_CL_SNEAKER_PU", "name": "Puma Running Shoes", "category": "clothing", "brand": "Puma"},
    {"id": "PROD_EL_HEAD_AP", "name": "Apple AirPods Pro", "category": "electronics", "brand": "Apple"},
    {"id": "PROD_EL_WATCH_SA", "name": "Samsung Galaxy Watch 5", "category": "electronics", "brand": "Samsung"},
    {"id": "PROD_CL_JEANS_LK", "name": "Levi's 501 Original Fit Jeans", "category": "clothing", "brand": "Levi's"},
    {"id": "PROD_BK_FANTASY_TR", "name": "The Realm of Eldoria (Fantasy)", "category": "books", "brand": "Tor Books"},
    {"id": "PROD_HM_LAMP_PH", "name": "Philips Hue Smart Bulb", "category": "home", "brand": "Philips"},
    {"id": "PROD_EL_TV_LG", "name": "LG OLED Smart TV", "category": "electronics", "brand": "LG"},
    {"id": "PROD_CL_HOODIE_NK", "name": "Nike Tech Fleece Hoodie", "category": "clothing", "brand": "Nike"},
    {"id": "PROD_BK_MYSTERY_AG", "name": "The Christie Affair (Mystery)", "category": "books", "brand": "HarperCollins"},
    {"id": "PROD_HM_SOFA_IK", "name": "IKEA Ektorp Sofa", "category": "home", "brand": "IKEA"},
    {"id": "PROD_EL_TABLET_IP", "name": "Apple iPad Air", "category": "electronics", "brand": "Apple"},
    {"id": "PROD_CL_DRESS_HM", "name": "H&M Summer Dress", "category": "clothing", "brand": "H&M"},
    {"id": "PROD_EL_SPEAKER_BO", "name": "Bose Bluetooth Speaker", "category": "electronics", "brand": "Bose"},
    {"id": "PROD_CL_SHIRT_AD", "name": "Adidas Training T-Shirt", "category": "clothing", "brand": "Adidas"},
    {"id": "PROD_BK_SCI_FI_PR", "name": "Project Hail Mary (Sci-Fi)", "category": "books", "brand": "Penguin Random House"},
    {"id": "PROD_HM_COFFEE_NV", "name": "Nespresso Vertuo Coffee Machine", "category": "home", "brand": "Nespresso"},
    {"id": "PROD_CL_JACKET_PU", "name": "Puma Windbreaker Jacket", "category": "clothing", "brand": "Puma"},
]
event_types = ["view_product", "add_to_cart", "purchase"]
sessions = [f"session_{i}" for i in range(1, 201)] # 200 simulated sessions

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

def generate_event():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    timestamp = now_utc.strftime("%Y-%m-%dT%H:%M:%S") + f".{now_utc.microsecond:06d}Z"
    user_id = random.choice(users)
    event_type = random.choice(event_types)
    product = random.choice(products)
    return {
        "timestamp": timestamp,
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product["id"],
        "category_id": product["category"],
        "brand": product["brand"],
        "product_name": product["name"],
        "session_id": random.choice(sessions)
    }

def simulate_attack():
    user_id = random.choice(users)
    for _ in range(random.randint(15, 30)): 
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        timestamp = now_utc.strftime("%Y-%m-%dT%H:%M:%S") + f".{now_utc.microsecond:06d}Z"
        product = random.choice(products)
        event = {
            "timestamp": timestamp,
            "user_id": user_id,
            "event_type": "view_product",
            "product_id": product["id"],
            "category_id": product["category"],
            "brand": product["brand"],
            "product_name": product["name"], 
            "session_id": random.choice(sessions)
        }
        event_data = EventData(json.dumps(event))
        producer.send_batch([event_data])
        time.sleep(random.uniform(0.01, 0.08)) 

with producer:
    while True:
        # Generate normal events
        num_normal_events = random.randint(5, 15) # Vary the number of normal events
        for _ in range(num_normal_events):
            event = generate_event()
            event_data = EventData(json.dumps(event))
            producer.send_batch([event_data])
            time.sleep(random.uniform(0.1, 0.5))  # More varied delay

        # Occasionally simulate an attack
        if random.random() < 0.05:  # Reduced probability of attack
            simulate_attack()
            print("simulate_attack")
        else:
            time.sleep(random.uniform(1, 3)) # Longer delay between normal activity periods