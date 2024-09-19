import random
import json
import time
from uuid import uuid4

from confluent_kafka import Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField


KAFKA_TOPIC = "click-stream"
kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})

user_ids = ["ba837232-f298-4628-843c-612832acc1db",
            "1f2f9578-a6aa-4413-9a46-e78ff8aa9fce",
            "007d816c-b173-4ccc-943f-d9dabd92aca5",
            "cd6b3b81-9783-45a9-bbc0-a1e0a9caf74a",
            "69759e6e-0c4d-4ed8-8391-821c1e8d076b",
            "46fd66d1-f463-4786-a52e-f9c07c11b5da",
            "d31cccfa-13f0-4ffc-8e08-097a38286294",
            "04cce658-64cf-40c6-be44-59984e1a9f73"
            "d0171a38-0b36-4191-b3e7-af5da7c84f38",
            "8acc651b-06ab-488d-8345-7e3d80f21cbf"]

categories_dict = {
    "Electronics": ["Smartphone", "Laptop", "Headphones"],
    "Clothing": ["T-shirt", "Jeans", "Dress"],
    "Home & Kitchen": ["Cookware Set", "Vacuum Cleaner"],
    "Beauty & Personal Care": ["Makeup Kit", "Perfume"],
    "Sports & Outdoors": ["Running Shoes", "Yoga Mat"],
    "Books": ["Novel", "Children's Book"],
    "Toys & Games": ["Toy Car", "Board Game"],
    "Automotive": ["Car Accessories", "Motor Oil"],
    "Health & Household": ["Vitamins", "Cleaning Supplies"],
    "Grocery & Gourmet Food": ["Snacks", "Coffee"]
}

products = [
    "Smartphone", "Laptop", "Headphones", "T-shirt", "Jeans", "Dress",
    "Cookware Set", "Vacuum Cleaner", "Makeup Kit", "Perfume", "Running Shoes",
    "Yoga Mat", "Novel", "Children's Book", "Toy Car", "Board Game",
    "Car Accessories", "Motor Oil", "Vitamins", "Cleaning Supplies", "Snacks", "Coffee"
]

events = [
    "CLICK",
    "VIEW",
    "SCROLL",
    "ADD_TO_CART",
    "REMOVE_FROM_CART",
    "PURCHASE_COMPLETED",
    "PURCHASE_ABANDONED",
    "LOGIN",
    "SIGNUP",
    "ACCOUNT_UPDATE",
    "NEWSLETTER_SUBSCRIPTION",
    "PAGE_VIEW",
    "ADD_TO_WISHLIST",
    "SHARE_PRODUCT",
    "WRITE_REVIEW",
    "AD_CLICK",
    "PROMOTION_CLICK",
    "COUPON_APPLY",
    "ERROR_PAGE_VIEW",
    "FORM_VALIDATION_ERROR"
]


"""
staging_payload = {
    "event_id":"",
    "timestamp":"",
    "user_id":"",
    "location":{
        "ip_address":"",
        "country":"",
        "city":""
    },
    "event_type":"CLICK/VIEW/SCROLL/ADD/REMOVE",
    "product_info":{
        "category":"",
        "product_name":""
    },
    "url":"url of the page this event occurred"
}
"""

# List of random IP addresses
ip_addresses = [
    "192.168.0.1", "10.0.0.1", "172.16.0.1", "8.8.8.8",
    "192.168.1.1", "10.0.0.2", "172.16.0.2", "8.8.4.4",
    # Add more IP addresses as needed
]

# Dictionary of countries and their cities
countries_and_cities = {
    "USA": ["New York", "Los Angeles", "Chicago", "Houston"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary"],
    "UK": ["London", "Manchester", "Birmingham", "Glasgow"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth"],
    "India": ["New Delhi", "Mumbai", "Bangalore", "Chennai"],
    # Add more countries and cities as needed
}

def generate_random_location():
    # Generate random IP address
    ip_address = random.choice(ip_addresses)

    # Generate random country and city
    country = random.choice(list(countries_and_cities.keys()))
    city = random.choice(countries_and_cities[country])

    return {
        "ip_address": ip_address,
        "country": country,
        "city": city
    }

def generate_random_product_info():
    category = random.choice(list(categories_dict.keys()))
    product_name = random.choice(categories_dict[category])
    return {
        "category": category,
        "product_name": product_name
    }

def simulate():
    random_location = generate_random_location()
    random_product = generate_random_product_info()
    payload_instance = {
        "event_id":str(uuid4()),
        "timestamp":int(time.time()),
        "user_id":random.choice(user_ids),
        "location":{
            "ip_address":random_location["ip_address"],
            "country":random_location["country"],
            "city":random_location["city"]
        },
        "event_type":random.choice(events),
        "product_info":{
            "category":random_product["category"],
            "product_name":random_product["product_name"]
        },
        "url":f"https://www.example.com/{random_product['category']}/{random_product['product_name']}"
    }
    return payload_instance

def delivery_callback(err, msg):
    if err:
        print('Message failed to deliver: %s' % err)
    else:
        print('Message delivered to %s' % msg.topic())
def publish_to_kafka(event):
    key = event["event_id"]
    value = event
    kafka_producer.produce(KAFKA_TOPIC, key=key, value=json.dumps(value), callback=delivery_callback)
    kafka_producer.flush()


if __name__ == "__main__":
    while True:
        nof_events, published_count = 1000, 0
        while published_count< nof_events:
            event = simulate()
            publish_to_kafka(event=event)
            published_count += 1
            
        print(f"Published {published_count} events")
        time.sleep(10)
