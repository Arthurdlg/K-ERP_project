from pymongo import MongoClient

# Replace the URI string with your MongoDB deployment's connection string.
client = MongoClient("mongodb://localhost:27017/")

# Select the database
db = client['KERP']

# Define the collections
users_collection = db['Users']
orders_collection = db['Orders']
notification_collection = db['Notification']
inventory_collection = db['Inventory']

# Sample data for Users
user_data = {
    "_id": 1,
    "UserName": "JohnDoe",
    "UserPassword": "password123",
    "UserMail": "johndoe@example.com"
}

# Sample data for Orders
order_data = {
    "_id": 1,
    "UserID": 1,
    "ProductID": 1,
    "Quantity": 2,
    "OrderDate": "2024-07-09",
    "OrderStatus": "Shipped",
    "Order_price": 49.99
}

# Sample data for Notification
notification_data = {
    "_id": 1,
    "UserID": 1,
    "Message": "Your order has been shipped!",
    "TimeStamp": "2024-07-09T12:34:56"
}

# Sample data for Inventory
inventory_data = {
    "_id": 1,
    "ProductName": "Widget",
    "Quantity": 100,
    "Price": 24.99
}

# Insert data into collections
users_collection.insert_one(user_data)
orders_collection.insert_one(order_data)
notification_collection.insert_one(notification_data)
inventory_collection.insert_one(inventory_data)

print("Data inserted successfully!")

