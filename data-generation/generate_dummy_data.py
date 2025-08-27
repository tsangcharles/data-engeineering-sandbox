#!/usr/bin/env python3
"""
Data Generation Script for Data Warehouse Course Project
Generates realistic dummy data for learning purposes
"""

import csv
import random
import datetime
from faker import Faker
import pandas as pd
import numpy as np

# Initialize Faker for realistic data
fake = Faker()
Faker.seed(42)  # For reproducible results

def generate_date_dimension(start_date, end_date):
    """Generate date dimension table data"""
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
        dates.append({
            'full_date': current_date.strftime('%Y-%m-%d'),
            'year': current_date.year,
            'quarter': (current_date.month - 1) // 3 + 1,
            'month': current_date.month,
            'month_name': current_date.strftime('%B'),
            'day_of_week': current_date.weekday() + 1,
            'day_name': current_date.strftime('%A'),
            'is_weekend': current_date.weekday() >= 5,
            'is_holiday': False  # Simplified for demo
        })
        current_date += datetime.timedelta(days=1)
    
    return dates

def generate_users(num_users=1000):
    """Generate user dimension data"""
    users = []
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'Japan', 'Brazil', 'India', 'China']
    cities = ['New York', 'London', 'Berlin', 'Paris', 'Tokyo', 'Sydney', 'Toronto', 'Mumbai', 'Beijing', 'Rio']
    
    for i in range(num_users):
        user = {
            'user_id': i + 1,
            'username': fake.user_name(),
            'email': fake.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'age': random.randint(18, 80),
            'gender': random.choice(['Male', 'Female', 'Other']),
            'country': random.choice(countries),
            'city': random.choice(cities),
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'is_active': random.choice([True, True, True, False]),  # 75% active
            'created_at': fake.date_time_between(start_date='-2y', end_date='-1d')
        }
        users.append(user)
    
    return users

def generate_products(num_products=500):
    """Generate product dimension data"""
    products = []
    
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
        'Clothing': ['Men', 'Women', 'Kids', 'Shoes'],
        'Home': ['Furniture', 'Kitchen', 'Decor', 'Garden'],
        'Books': ['Fiction', 'Non-Fiction', 'Academic', 'Children'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Equipment']
    }
    
    brands = ['TechCorp', 'StyleBrand', 'HomeStyle', 'BookWorld', 'SportMax', 'Generic', 'Premium', 'Budget']
    suppliers = ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D', 'Supplier E']
    
    for i in range(num_products):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        # Price ranges based on category
        if category == 'Electronics':
            base_price = random.uniform(50, 2000)
        elif category == 'Clothing':
            base_price = random.uniform(20, 300)
        elif category == 'Home':
            base_price = random.uniform(30, 800)
        elif category == 'Books':
            base_price = random.uniform(10, 100)
        else:  # Sports
            base_price = random.uniform(25, 500)
        
        cost = base_price * random.uniform(0.3, 0.7)  # Cost is 30-70% of price
        
        product = {
            'product_id': i + 1,
            'product_name': f"{fake.word().title()} {fake.word().title()}",
            'category': category,
            'subcategory': subcategory,
            'price': round(base_price, 2),
            'cost': round(cost, 2),
            'brand': random.choice(brands),
            'supplier': random.choice(suppliers),
            'created_at': fake.date_time_between(start_date='-1y', end_date='-1d')
        }
        products.append(product)
    
    return products

def generate_user_activity_logs(users, num_logs=10000):
    """Generate user activity log data"""
    logs = []
    
    page_urls = [
        '/home', '/products', '/cart', '/checkout', '/account', '/search', 
        '/category/electronics', '/category/clothing', '/product/', '/about'
    ]
    
    action_types = ['page_view', 'click', 'search', 'add_to_cart', 'remove_from_cart', 'login', 'logout']
    
    for i in range(num_logs):
        user = random.choice(users)
        page_url = random.choice(page_urls)
        if page_url == '/product/':
            page_url += str(random.randint(1, 500))
        
        log = {
            'log_id': i + 1,
            'user_id': user['user_id'],
            'session_id': fake.uuid4(),
            'page_url': page_url,
            'action_type': random.choice(action_types),
            'timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
            'ip_address': fake.ipv4(),
            'user_agent': fake.user_agent(),
            'response_time_ms': random.randint(50, 2000),
            'status_code': random.choice([200, 200, 200, 404, 500, 302])  # Mostly 200s
        }
        logs.append(log)
    
    return logs

def generate_transactions(users, products, num_transactions=5000):
    """Generate transaction data"""
    transactions = []
    
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay']
    transaction_statuses = ['Completed', 'Completed', 'Completed', 'Pending', 'Failed', 'Refunded']
    
    for i in range(num_transactions):
        user = random.choice(users)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        unit_price = product['price']
        total_amount = quantity * unit_price
        
        transaction = {
            'transaction_id': i + 1,
            'user_id': user['user_id'],
            'product_id': product['product_id'],
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'transaction_date': fake.date_time_between(start_date='-30d', end_date='now'),
            'payment_method': random.choice(payment_methods),
            'transaction_status': random.choice(transaction_statuses),
            'shipping_address': fake.address()
        }
        transactions.append(transaction)
    
    return transactions

def generate_clickstream(users, products, num_clicks=8000):
    """Generate clickstream data"""
    clicks = []
    
    page_types = ['product', 'category', 'search', 'home', 'cart', 'checkout']
    referrers = ['google.com', 'facebook.com', 'twitter.com', 'instagram.com', 'direct', 'email']
    device_types = ['Desktop', 'Mobile', 'Tablet']
    browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
    
    for i in range(num_clicks):
        user = random.choice(users)
        product = random.choice(products)
        
        click = {
            'click_id': i + 1,
            'user_id': user['user_id'],
            'product_id': product['product_id'],
            'page_type': random.choice(page_types),
            'click_timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
            'referrer_url': random.choice(referrers),
            'device_type': random.choice(device_types),
            'browser': random.choice(browsers),
            'time_on_page_seconds': random.randint(5, 1800)  # 5 seconds to 30 minutes
        }
        clicks.append(click)
    
    return clicks

def save_to_csv(data, filename, fieldnames=None):
    """Save data to CSV file"""
    if not data:
        return
    
    if fieldnames is None:
        fieldnames = data[0].keys()
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Generated {len(data)} records in {filename}")

def main():
    """Main function to generate all dummy data"""
    print("Generating dummy data for Data Warehouse Course Project...")
    
    # Create data directory if it doesn't exist
    import os
    os.makedirs('data', exist_ok=True)
    
    # Generate dimension data
    print("Generating date dimension...")
    start_date = datetime.date(2022, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    dates = generate_date_dimension(start_date, end_date)
    save_to_csv(dates, 'data/dates.csv')
    
    print("Generating users...")
    users = generate_users(1000)
    save_to_csv(users, 'data/users.csv')
    
    print("Generating products...")
    products = generate_products(500)
    save_to_csv(products, 'data/products.csv')
    
    # Generate fact data
    print("Generating user activity logs...")
    logs = generate_user_activity_logs(users, 10000)
    save_to_csv(logs, 'data/user_activity_logs.csv')
    
    print("Generating transactions...")
    transactions = generate_transactions(users, products, 5000)
    save_to_csv(transactions, 'data/transactions.csv')
    
    print("Generating clickstream data...")
    clicks = generate_clickstream(users, products, 8000)
    save_to_csv(clicks, 'data/clickstream.csv')
    
    print("\nData generation complete! Files saved in 'data/' directory.")
    print("\nGenerated files:")
    print("- dates.csv: Date dimension table")
    print("- users.csv: User dimension table")
    print("- products.csv: Product dimension table")
    print("- user_activity_logs.csv: User activity logs")
    print("- transactions.csv: Transaction data")
    print("- clickstream.csv: Clickstream data")

if __name__ == "__main__":
    main()
