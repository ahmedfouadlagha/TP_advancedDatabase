import pymongo
from pymongo import MongoClient
import pprint
import random

# Connect to the MongoDB server
client = MongoClient("mongodb://localhost:27017/")
# Create a database called "taxiradio"
db = client.taxiradio # db = client["taxiradio"]

# Create collections for each entity
cars = db["Cars"] #collection = db["test-collection"]
customers = db["Customers"]
drivers = db["Drivers"]
locations = db["Locations"]
routes = db["Routes"]
trips = db["Trips"]

# Insert multiple documents into the collection Cars
for i in range(1, 101):
    car = {  
        "_id": "CA"+str(i).zfill(4),
        "REGISTRATION NUMBER": str(random.randint(11111,9999999))+" 1"+str(random.randint(10,23))+" "+str(random.randint(1,58)),
        "DESCRIPTION": random.choice(["BMW Serie 3 F30","Citroën C3","Dacia Duster 2","Peugeot 2008","Renault Captur 2","Volkswagen Polo","Volkswagen Golf 8", "Volkswagen Golf 7","Renault Megane 4","Peugeot 3008","TOYOTA COROLLA 12","AUDI A5","Audi Rs4","Chevrolet Camaro","Fiat 500","Fiat Topolino","Ford Mustang","Jaguar E-Type","Lamborghini Miura","Hyundai Bayon","Kia EV6","Kia Sorento","Kia Sportage","Kia Stonic","Kia XCeed","Land Rover Defender","Land Rover Discovery","Land Rover Range Rover","Mazda MX-30","Mazda MX-5","Mercedes Classe C","Mercedes Classe E","Mercedes Classe S","Mercedes Classe E","Hyundai I30"]),
        "TYPE": random.choice(["manual","automatic"]),
        "MODEL": random.choice(["2015","2016","2017","2018","2019","2020"]),
        "COLOR": random.choice(["Red","Blue","Black","White","Silver","Grey","Yellow","Green","Orange","Purple","Pink","Brown","Beige","Gold","Bronze","Copper","Khaki","Turquoise","Aquamarine","Teal","Azure","Crimson","Lime","Olive","Indigo","Violet","Magenta","Maroon","Navy","Tan","Lavender","Plum","Coral","Salmon","Ivory","Cyan","Fuchsia","Wheat","Lemon","Apricot","Lilac","Peach","Mauve","Mint","Lavender","Ocher","Carmine","Cerulean","Cobalt","Cinnamon","Coral","Cyan","Emerald","Ginger","Lemon","Lilac","Lime","Magenta","Mauve","Olive","Ocher","Peach","Plum","Salmon","Sapphire","Scarlet","Silver","Tan","Teal","Turquoise","Violet","Wheat","White","Yellow"]),
        "CAPACITY": random.randint(3,12),
        "STATUS": random.choice(["Available","Busy","On Trip","On Break"]),
        "LOCATION": "L"+str(random.randint(1,3)).zfill(4),
        "DRIVER": "DR"+str(random.randint(1,3)).zfill(4),
        "ROUTE": "R"+str(random.randint(1,3)).zfill(4)
    }
    cars.insert_one(car)

# Insert multiple documents into the collection Customers
for i in range(1, 10001):
    first_name = random.choice(["Mohamed","Ahmed","Ali","Omar","Youssef","Abdelrahman","Khaled","Mahmoud","Amr","Hassan","Hussein","Abdalla","Abdallah","Abdulrahman","Abdulrahim","Akrem","Ali","Fouad","Tarek","Islem","Ibrahim"])
    last_name = random.choice(["Mohamed","Ahmed","Ali","Omar","Youssef","Abdelrahman","Khaled","Mahmoud","Amr","Hassan","Hussein","Abdalla","Abdallah","Abdulrahman","Abdulrahim","Akrem","Ali","Fouad","Tarek","Islem","Ibrahim"])
    customer = {  
        "_id": "CU"+str(i).zfill(4),
        "FIRST NAME": first_name,
        "LAST NAME": last_name,
        "PHONE NUMBER": "06"+str(random.randint(1,9)),
        "EMAIL": first_name+"."+last_name+"@gmail.com",
        "CARD DETAILS": {
            "CARD ID": "CA"+str(i).zfill(4),
            "CARD TYPE": random.choice(["Visa","MasterCard","American Express","Discover","JCB","Diners Club","UnionPay"]),
            "CARD NUMBER": str(random.randint(1111111111111111,9999999999999999)),
            "CARD HOLDER": first_name+" "+last_name,
            "EXPIRATION DATE": str(random.randint(1,12))+"/"+str(random.randint(2022,2025)),
            "CVV": str(random.randint(111,999))
        },
    }
    customers.insert_one(customer)
  
# Insert multiple documents into the collection Drivers
for i in range(1, 121):
    first_name = random.choice(["Mohamed","Ahmed","Ali","Omar","Youssef","Abdelrahman","Khaled","Mahmoud","Amr","Hassan","Hussein","Abdalla","Abdallah","Abdulrahman","Abdulrahim","Akrem","Ali","Fouad","Tarek","Islem","Ibrahim"])
    last_name = random.choice(["Mohamed","Ahmed","Ali","Omar","Youssef","Abdelrahman","Khaled","Mahmoud","Amr","Hassan","Hussein","Abdalla","Abdallah","Abdulrahman","Abdulrahim","Akrem","Ali","Fouad","Tarek","Islem","Ibrahim"])
    driver = {  
        "_id": "DR"+str(i).zfill(4),
        "FIRST NAME": first_name,
        "LAST NAME": last_name,
        "PHONE NUMBER": "06"+str(random.randint(1,9)),
        "EMAIL": first_name+last_name+"@gmail.com",
        "CAR": "CA"+str(i).zfill(4),
        "ROUTE": "R"+str(random.randint(1,3)).zfill(4),
        "LOCATION": "L"+str(random.randint(1,3)).zfill(4),
        "STATUS": random.choice(["Available","Busy","On Trip","On Break"]),
        "TRIP": "TR"+str(random.randint(1,3)).zfill(4)
    }
    drivers.insert_one(driver)

# Insert multiple documents into the collection Locations
for i in range(1, 20001):
    location = {  
        "_id": "L"+str(i).zfill(4),
        "STREET": random.choice(["4 rue Kerrouche Slimane, 16105","Lotissement 1er Novembre n°4 BP 74, 16303","13 rue Ahmed Boumeda, 06000","57 rue des Cousins Gouraya, 16105","Surcouf Plage, 16612","76 rue Didouche Mourad","8 rue n°3 lot Boirie, 16208 Kouba"]),
        "CITY": random.choice(["Alger","Oran","Constantine","Annaba","Blida","Batna","Djelfa","Sétif","Sidi Bel Abbès","Biskra","Tébessa","El Oued","Skikda","Tiaret","Béjaïa","Tlemcen","Ouargla","Béchar","Mostaganem","Bordj Bou Arreridj","Chlef","Souk Ahras","Médéa","Tizi Ouzou","Saïda","Guelma","Relizane","Aïn Béïda","Khenchela","Mascara","Tissemsilt","El Eulma","Bou Saâda","Aïn Ounass","Tamanrasset","Bouïra","Tindouf","El Bayadh","Aïn Témouchent","Ghardaïa","Timimoun","In Salah","In Guezzam","In Amguel","In Ecker","In Zghmir","In Mlili","In Mghila","In Ghar"]),
        "WILAYA": random.choice(["Alger","Oran","Constantine","Annaba","Blida","Batna","Djelfa","Sétif","Sidi Bel Abbès","Biskra","Tébessa","El Oued","Skikda","Tiaret","Béjaïa","Tlemcen","Ouargla","Béchar","Mostaganem","Bordj Bou Arreridj","Chlef","Souk Ahras","Médéa","Tizi Ouzou","Saïda","Guelma","Relizane","Aïn Béïda","Khenchela","Mascara","Tissemsilt","El Eulma","Bou Saâda","Aïn Ounass","Tamanrasset","Bouïra","Tindouf","El Bayadh","Aïn Témouchent","Ghardaïa","Timimoun","In Salah","In Guezzam","In Amguel","In Ecker","In Zghmir","In Mlili","In Mghila","In Ghar"]),
        "COORDINATES": {
            "TYPE": "Point",
            "COORDINATES": [
                round(random.uniform(-100,100), 6),
                round(random.uniform(-100,100), 6)
            ],
        },
        "DRIVER": "DR0000"+str(random.randint(1,3)),
        "CAR": "CA"+str(i).zfill(4),
        "ROUTE": "RO0000"+str(random.randint(1,3)),
        "TRIP": "TR0000"+str(random.randint(1,3))
    }
    locations.insert_one(location)

# Insert multiple documents into the collection Routes
trip = []
for i in range(1, 9001):
    for j in range(1, random.randint(1,5)):
        trip.append("TR"+str(random.randint(1,9999)).zfill(4))
    
    route = {  
        "_id": "RO"+str(i).zfill(4),
        "START": "L"+str(random.randint(1,9999)),
        "END": "L"+str(random.randint(1,9999)),
        "DISTANCE": random.randint(1,100),
        "DURATION": random.randint(1,100),
        "DRIVER": "DR0000"+str(random.randint(1,3)),
        "CAR": "CA"+str(i).zfill(4),
        "TRIP": trip
    }
    routes.insert_one(route)

# Insert multiple documents into the collection Trips
for i in range(1, 20001):
    trip = {  
        "_id": "TR"+str(i).zfill(4),
        "SOURCE": "L"+str(random.randint(1,9999)),
        "DESTINATION": "L"+str(random.randint(1,9999)),
        "DISTANCE": random.uniform(1,100),
        "PRICE": random.uniform(1,100),
        "DURATION": random.randint(1,100),
        "SHARING": random.choice(["Yes","No"]),
        "DRIVER": "DR"+str(random.randint(1,9999)).zfill(4),
        "CAR": "CA"+str(i).zfill(4),
        "ROUTE": "RO"+str(random.randint(1,9999).zfill(4)),
        "CUSTOMER": "CU"+str(random.randint(1,9999)).zfill(4),
        "PAYMENT": random.choice(["Cash","Card"]),
        "STATUS": random.choice(["Pending","Completed","Cancelled"])
    }
    trips.insert_one(trip)




client.close()


