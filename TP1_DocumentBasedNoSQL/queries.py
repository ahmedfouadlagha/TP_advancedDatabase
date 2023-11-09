import pymongo
from pymongo import MongoClient
import pprint
import random

# Connect to the MongoDB server
client = MongoClient("mongodb://localhost:27017/")
# Connect to the database
db = client["taxiradio"]
# Connect to the collection
cars = db["Cars"] 
customers = db["Customers"]
drivers = db["Drivers"]
locations = db["Locations"]
routes = db["Routes"]
trips = db["Trips"]

# Find all documents
def find_all():
    print("Find all documents:")
    for car in cars.find():
        print(car)

# Find one document
def find_one():
    print("Find one document:")
    print(cars.find_one())

# Find documents with a filter
def find_filter():
    print("Find documents with a filter:")
    for doc in cars.find({"color": "red"}):
        print(doc)

# Find documents with a filter and projection
def find_filter_projection():
    print("Find documents with a filter and projection:")
    for doc in cars.find({"color": "red"}, {"_id": 0, "brand": 1, "model": 1}):
        print(doc)

def find_filter_sort():
    print("Find documents with a filter and sort:")
    for doc in cars.find({"color": "red"}).sort("model", pymongo.DESCENDING):
        print(doc)

def find_filter_limit():
    print("Find documents with a filter and limit:")
    for doc in cars.find({"color": "red"}).limit(2):
        print(doc)

def find_filter_skip():
    print("Find documents with a filter and skip:")
    for doc in cars.find({"color": "red"}).skip(2):
        print(doc)

def find_filter_count():
    print("Find documents with a filter and count:")
    print(cars.find({"color": "red"}).count())

def find_filter_distinct():
    print("Find documents with a filter and distinct:")
    print(cars.find({"color": "red"}).distinct("model"))

def find_filter_aggregate():
    print("Find documents with a filter and aggregate:")
    print(cars.aggregate([{"$match": {"color": "red"}}, {"$count": "Total"}]))

def find_filter_lookup():
    print("Find documents with a filter and lookup:")
    print(cars.aggregate([{"$match": {"color": "red"}}, {"$lookup": {"from": "customers", "localField": "model", "foreignField": "model", "as": "customers"}}]))

def find_filter_unwind():
    print("Find documents with a filter and unwind:")
    print(cars.aggregate([{"$match": {"color": "red"}}, {"$unwind": "$model"}]))

def find_filter_group():
    print("Find documents with a filter and group:")
    print(cars.aggregate([{"$match": {"color": "red"}}, {"$group": {"_id": "$model", "count": {"$sum": 1}}}]))

def find_filter_sample():
    print("Find documents with a filter and sample:")
    print(cars.aggregate([{"$match": {"color": "red"}}, {"$sample": {"size": 2}}]))


# Print the number of documents in each collection
print("Number of documents in the collection Cars: ", cars.count_documents({}))
print("Number of documents in the collection Customers: ", customers.count_documents({}))
print("Number of documents in the collection Drivers: ", drivers.count_documents({}))
print("Number of documents in the collection Locations: ", locations.count_documents({}))
print("Number of documents in the collection Routes: ", routes.count_documents({}))
print("Number of documents in the collection Trips: ", trips.count_documents({}))

# Print the first document in each collection
print("First document in the collection Cars: ", cars.find_one())
print("First document in the collection Customers: ", customers.find_one())
print("First document in the collection Drivers: ", drivers.find_one())
print("First document in the collection Locations: ", locations.find_one())
print("First document in the collection Routes: ", routes.find_one())
print("First document in the collection Trips: ", trips.find_one())

# Print the last document in each collection
print("Last document in the collection Cars: ", cars.find_one(sort=[("_id", pymongo.DESCENDING)]))
print("Last document in the collection Customers: ", customers.find_one(sort=[("_id", pymongo.DESCENDING)]))
print("Last document in the collection Drivers: ", drivers.find_one(sort=[("_id", pymongo.DESCENDING)]))
print("Last document in the collection Locations: ", locations.find_one(sort=[("_id", pymongo.DESCENDING)]))
print("Last document in the collection Routes: ", routes.find_one(sort=[("_id", pymongo.DESCENDING)]))
print("Last document in the collection Trips: ", trips.find_one(sort=[("_id", pymongo.DESCENDING)]))

# Print the first 5 documents in each collection 
print("First 5 documents in the collection Cars: ")
for car in cars.find({}, {"_id": 0, "DESCRIPTION": 1}).limit(5):
    pprint.pprint(car)
print("First 5 documents in the collection Customers: ")
for customer in customers.find({}, {"_id": 0, "FIRST NAME": 1}).limit(5):
    pprint.pprint(customer)
print("First 5 documents in the collection Drivers: ")
for driver in drivers.find({}, {"_id": 0, "EMAIL": 1}).limit(5):
    pprint.pprint(driver)
print("First 5 documents in the collection Locations: ")
for location in locations.find({}, {"_id": 0, "CITY": 1}).limit(5):
    pprint.pprint(location)
print("First 5 documents in the collection Routes: ")
for route in routes.find({}, {"_id": 0, "DURATION": 1}).limit(5):
    pprint.pprint(route)
print("First 5 documents in the collection Trips: ")
for trip in trips.find({}, {"_id": 0, "PRICE": 1}).limit(5):
    pprint.pprint(trip)
    
# Print the last 5 documents in each collection
print("Last 5 documents in the collection Cars: ")
for car in cars.find().sort([("_id", pymongo.DESCENDING)]).limit(5):
    pprint.pprint(car)
print("Last 5 documents in the collection Customers: ")
for customer in customers.find().sort([("_id", pymongo.DESCENDING)]).limit(5):
    pprint.pprint(customer)
print("Last 5 documents in the collection Drivers: ")
for driver in drivers.find().sort([("_id", pymongo.DESCENDING)]).limit(5):
    pprint.pprint(driver)
print("Last 5 documents in the collection Locations: ")
for location in locations.find().sort([("_id", pymongo.DESCENDING)]).limit(5):
    pprint.pprint(location)
print("Last 5 documents in the collection Routes: ")
for route in routes.find().sort([("_id", pymongo.DESCENDING)]).limit(5):
    pprint.pprint(route)
print("Last 5 documents in the collection Trips: ")
for trip in trips.find().sort([("_id", pymongo.DESCENDING)]).limit(5):
    pprint.pprint(trip)

# Print the documents with the highest and lowest price in the collection Trips
print("Documents with the highest and lowest price in the collection Trips: ")
for trip in trips.find().sort([("PRICE", pymongo.DESCENDING)]).limit(1):
    pprint.pprint(trip)
for trip in trips.find().sort([("PRICE", pymongo.ASCENDING)]).limit(1):
    pprint.pprint(trip)

# Print the documents with the highest and lowest distance in the collection Trips
print("Documents with the highest and lowest distance in the collection Trips: ")
for trip in trips.find().sort([("DISTANCE", pymongo.DESCENDING)]).limit(1):
    pprint.pprint(trip)
for trip in trips.find().sort([("DISTANCE", pymongo.ASCENDING)]).limit(1):
    pprint.pprint(trip)

# Print the documents with the highest and lowest duration in the collection Trips
print("Documents with the highest and lowest duration in the collection Trips: ")
for trip in trips.find().sort([("DURATION", pymongo.DESCENDING)]).limit(1):
    pprint.pprint(trip)
for trip in trips.find().sort([("DURATION", pymongo.ASCENDING)]).limit(1):
    pprint.pprint(trip)

# Count the number of documents in the collection that match a specific criteria
cars.count_documents({"CAPACITY": 7})
cars.count_documents({"CAPACITY": {"$gt": 6}})
cars.count_documents({"CAPACITY": {"$gte": 6}})
cars.count_documents({"CAPACITY": {"$lt": 6}})
cars.count_documents({"CAPACITY": {"$lte": 6}})
cars.count_documents({"CAPACITY": {"$ne": 6}})
cars.count_documents({"CAPACITY": {"$in": [4, 8]}})
cars.count_documents({"CAPACITY": {"$nin": [4, 8]}})
cars.count_documents({"CAPACITY": {"$exists": True}})
cars.count_documents({"CAPACITY": {"$exists": False}})
cars.count_documents({"DESCRIPTION":"Fiat 500", "TYPE": "automatic"})
cars.count_documents({"DESCRIPTION": {"$in": ["Land Rover Range Rover", "Kia Stonic"]}})
cars.count_documents({"$or": [{"DESCRIPTION": "Kia Stonic"}, {"TYPE": "automatic"}]})
cars.count_documents({"$and": [{"DESCRIPTION": "Kia Stonic"}, {"TYPE": "automatic"}]})
cars.count_documents({"$nor": [{"DESCRIPTION": "Kia Stonic"}, {"TYPE": "automatic"}]})
cars.count_documents({"$not": {"DESCRIPTION": "Kia Stonic"}})

# Update a document in the collection
cars.update_one({"_id": "CA00001"}, {"$set": {"DESCRIPTION": "Maruti Swift Desire"}})

# aggregate() method
# Print the total number of documents in each collection
print("Total number of documents in the collection Cars: ", cars.aggregate([{"$count": "Total"}]))
print("Total number of documents in the collection Customers: ", customers.aggregate([{"$count": "Total"}]))
print("Total number of documents in the collection Drivers: ", drivers.aggregate([{"$count": "Total"}]))
print("Total number of documents in the collection Locations: ", locations.aggregate([{"$count": "Total"}]))
print("Total number of documents in the collection Routes: ", routes.aggregate([{"$count": "Total"}]))
print("Total number of documents in the collection Trips: ", trips.aggregate([{"$count": "Total"}]))

print(cars.distinct("DESTINATION"))

#lookup() method
print("Find documents with a filter and lookup:")
pipeline = [
    {"$match": 
        {"color": "red"}},
    {"$lookup": 
        {"from": "Drivers", 
         "localField": "DRIVER", 
         "foreignField": "_id", 
         "as": "owner"}}
]
print(cars.aggregate(pipeline))

# add the the total cars that driver can drive to Drivers collection
pipeline = [
    {"$group": 
        {"_id": "$DRIVER", 
         "total": {"$sum": 1}}}
]
print(cars.aggregate(pipeline))


# Delete a document from the collection
cars.delete_one({"_id": "CA00001"})

# Delete all documents from the collection
cars.delete_many({})

# Delete the collection
cars.drop()

# Delete the database#
client.drop_database("taxiradio")

# Close the connection
client.close()
