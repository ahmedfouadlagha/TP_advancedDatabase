import psycopg2
import pandas as pd
from pandas.tseries.offsets import MonthBegin, MonthEnd
# Connect to an existing database
connection = psycopg2.connect(
    host='localhost',
    port='5432',
    database='SMS_db',
    user='postgres', 
    password='root'
)

# Open a cursor to perform database operations
cursor = connection.cursor()
# Drop table 'users' if it already exists
cursor.execute("DROP TABLE IF EXISTS users;")
# Create table 'users'
query = '''CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    user_first_name VARCHAR(30) NOT NULL,
    user_last_name VARCHAR(30) NOT NULL,
    user_email_id VARCHAR(50) NOT NULL,
    user_email_validated BOOLEAN DEFAULT FALSE,
    user_password VARCHAR(200),
    user_role VARCHAR(1) NOT NULL DEFAULT 'U', --U and A
    is_active BOOLEAN DEFAULT FALSE,
    created_dt DATE DEFAULT CURRENT_DATE,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);'''
cursor.execute(query)
##################################PARTIONNEMENT PAR LIST##################################
# Drop table 'users_part' if it already exists
cursor.execute("DROP TABLE IF EXISTS users_part;")
# Create table 'users_part' with partition by list
query = '''CREATE TABLE users_part (
    user_id SERIAL,
    user_first_name VARCHAR(30) NOT NULL,
    user_last_name VARCHAR(30) NOT NULL,
    user_email_id VARCHAR(50) NOT NULL,
    user_email_validated BOOLEAN DEFAULT FALSE,
    user_password VARCHAR(200),
    user_role VARCHAR(1) NOT NULL DEFAULT 'U', --U and A
    is_active BOOLEAN DEFAULT FALSE,
    created_dt DATE DEFAULT CURRENT_DATE,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_role, user_id)
)PARTITION BY LIST(user_role);'''
cursor.execute(query)
# create table 'users_part_default' for default partition
query = '''CREATE TABLE users_part_default PARTITION OF users_part DEFAULT;'''
cursor.execute(query)
# insert data into 'user_part' table
query = '''INSERT INTO users_part (user_first_name, user_last_name, user_email_id, user_role) 
VALUES 
    ('Tarek', 'Lalaouna', 'lalaouna.mohamed.tarek@univ-khenchela.dz', 'U'),
    ('Sarah', 'Bouchareb', 'sarah.bouchareb@yahoo.fr', 'U'),
    ('Fouad', 'Lagha', 'laghaahmedfouad@gmail.com', 'U');'''
cursor.execute(query)
# result
query = '''SELECT * FROM users_part_default;'''
cursor.execute(query)
print(cursor.fetchall())

# create the partition tables for user_role = 'A'
query = '''CREATE TABLE users_part_a 
        PARTITION OF users_part 
        FOR VALUES IN ('A');'''
cursor.execute(query)
#update the user_role = 'A' of user_email_id = 'lalaouna.mohamed.tarek@univ-khenchela.dz'
query = '''UPDATE users_part 
        SET 
            user_role = 'A'  
        WHERE user_email_id = 'lalaouna.mohamed.tarek@univ-khenchela.dz';'''
cursor.execute(query)
query = '''SELECT * FROM users_part;'''
cursor.execute(query)
print(cursor.fetchall())

query = '''SELECT * FROM users_part_a;'''
cursor.execute(query)
print(cursor.fetchall())

query = '''SELECT * FROM users_part_default;'''
cursor.execute(query)
print(cursor.fetchall())

# create the partition tables for user_role = 'U'
query = '''ALTER TABLE users_part
        DETACH PARTITION users_part_default;'''
cursor.execute(query)

query = '''CREATE TABLE users_part_u 
        PARTITION OF users_part 
        FOR VALUES IN ('U');'''
cursor.execute(query)
#insert data into user_part table
query = '''INSERT INTO users_part
SELECT * FROM users_part_default;'''
cursor.execute(query)

query = '''SELECT * FROM users_part_a;'''
cursor.execute(query)
print(cursor.fetchall())

query = '''SELECT * FROM users_part_u;'''
cursor.execute(query)
print(cursor.fetchall())
# drop and recreate the default partition table
query = '''DROP TABLE users_part_default;'''
cursor.execute(query)
query = '''CREATE TABLE users_part_default
        PARTITION OF users_part DEFAULT;'''
cursor.execute(query)

##################################PARTIONNEMENT PAR RANGE##################################
# Drop table 'users_range_part' if it already exists
cursor.execute("DROP TABLE IF EXISTS users_range_part;")
# Create table 'users_range_part' with partition by range
query = '''CREATE TABLE users_range_part (
    user_id SERIAL,
    user_first_name VARCHAR(30) NOT NULL,
    user_last_name VARCHAR(30) NOT NULL,
    user_email_id VARCHAR(50) NOT NULL,
    user_email_validated BOOLEAN DEFAULT FALSE,
    user_password VARCHAR(200),
    user_role VARCHAR(1) NOT NULL DEFAULT 'U', --U and A
    is_active BOOLEAN DEFAULT FALSE,
    created_dt DATE DEFAULT CURRENT_DATE,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (created_dt, user_id)
) PARTITION BY RANGE(created_dt);'''
cursor.execute(query)
# create table 'users_range_part_default' for default partition
query = '''CREATE TABLE users_range_part_default
        PARTITION OF users_range_part DEFAULT;'''
cursor.execute(query) 

# create the partition tables for created_dt 2016
query = '''CREATE TABLE users_range_part_2016
PARTITION OF users_range_part
FOR VALUES FROM ('2016-01-01') TO ('2016-12-31');'''
cursor.execute(query)
# create the partition tables for created_dt 2017
query = '''CREATE TABLE users_range_part_2017
PARTITION OF users_range_part
FOR VALUES FROM ('2017-01-01') TO ('2017-12-31');'''
cursor.execute(query)
# create the partition tables for created_dt 2018
query = '''CREATE TABLE users_range_part_2018
PARTITION OF users_range_part
FOR VALUES FROM ('2018-01-01') TO ('2018-12-31');'''
cursor.execute(query)
# create the partition tables for created_dt 2019
query = '''CREATE TABLE users_range_part_2019
PARTITION OF users_range_part
FOR VALUES FROM ('2019-01-01') TO ('2019-12-31');'''
cursor.execute(query)
# create the partition tables for created_dt 2020
query = '''CREATE TABLE users_range_part_2020
PARTITION OF users_range_part
FOR VALUES FROM ('2020-01-01') TO ('2020-12-31');'''
cursor.execute(query)
#insert data 
query = '''INSERT INTO users_range_part 
    (user_first_name, user_last_name, user_email_id, created_dt)
VALUES 
    ('Tarek', 'lalaouna', 'tarek@outlook.com', '2018-10-01'),
    ('fouad', 'lagha', 'fouad@gmail.com', '2019-02-10'),
    ('sarah', 'bouchareb', 'sarah@yahoo.fr', '2017-06-22');'''
cursor.execute(query)
#selection
query = '''SELECT user_first_name, user_last_name, user_email_id, created_dt
        FROM users_range_part_default;'''
cursor.execute(query)

query = '''SELECT user_first_name, user_last_name, user_email_id, created_dt
        FROM users_range_part_2017;'''
cursor.execute(query)

query = '''SELECT user_first_name, user_last_name, user_email_id, created_dt
        FROM users_range_part_2018;'''
cursor.execute(query)

query = '''SELECT user_first_name, user_last_name, user_email_id, created_dt
        FROM users_range_part_2019;'''
cursor.execute(query)

query = '''SELECT user_first_name, user_last_name, user_email_id, created_dt
        FROM users_range_part_2020;'''
cursor.execute(query)
##################################################################################
##################################range using PLSQL##################################
#Detach all yearly partitions
query = '''ALTER TABLE users_range_part
        DETACH PARTITION users_range_part_default;'''
cursor.execute(query)
query = '''ALTER TABLE users_range_part
        DETACH PARTITION users_range_part_2016;'''
cursor.execute(query)
query = '''ALTER TABLE users_range_part
        DETACH PARTITION users_range_part_2017;'''
cursor.execute(query)
query = '''ALTER TABLE users_range_part
        DETACH PARTITION users_range_part_2018;'''
cursor.execute(query)
query = '''ALTER TABLE users_range_part
        DETACH PARTITION users_range_part_2019;'''
cursor.execute(query)
query = '''ALTER TABLE users_range_part
        DETACH PARTITION users_range_part_2020;'''
cursor.execute(query)
# Add new partitions for every month between 2016 January and 2020 December.
months = pd.date_range(start='1/1/2016', end='12/31/2020', freq='1M')
table_name = 'users_range_part'
query = '''
CREATE TABLE {table_name}_{yyyymm}
PARTITION OF {table_name}
FOR VALUES FROM ('{begin_date}') TO ('{end_date}')
'''
for month in months:
    begin_date = month - MonthBegin(1)
    end_date = month + MonthEnd(0)
    print(f'Adding partition for {begin_date} and {end_date}')
    cursor.execute(
        query.format(
            table_name=table_name,
            yyyymm=str(month)[:7].replace('-', ''),
            begin_date=str(begin_date).split(' ')[0],
            end_date=str(end_date).split(' ')[0]
        ), ()
    )
#Load data from detached yearly partitions into monthly partitioned table.
query = '''INSERT INTO users_range_part
SELECT * FROM users_range_part_default;'''
cursor.execute(query)
query = '''INSERT INTO users_range_part
SELECT * FROM users_range_part_2016;'''
cursor.execute(query)
query = '''INSERT INTO users_range_part
SELECT * FROM users_range_part_2017;'''
cursor.execute(query)
query = '''INSERT INTO users_range_part
SELECT * FROM users_range_part_2018;'''
cursor.execute(query)
query = '''INSERT INTO users_range_part
SELECT * FROM users_range_part_2019;'''
cursor.execute(query)
query = '''INSERT INTO users_range_part
SELECT * FROM users_range_part_2020;'''
cursor.execute(query)
#selection
query='''SELECT * FROM users_range_part;'''
cursor.execute(query)
print(cursor.fetchall())

query = '''SELECT * FROM users_range_part_201706'''
cursor.execute(query)
print(cursor.fetchall())

query = '''SELECT * FROM users_range_part_201810'''
cursor.execute(query)
print(cursor.fetchall())

query = '''SELECT * FROM users_range_part_201902'''
cursor.execute(query)
print(cursor.fetchall())

query = '''DROP TABLE users_range_part_2016'''
cursor.execute(query)
query = '''DROP TABLE users_range_part_2017'''
cursor.execute(query)
query = '''DROP TABLE users_range_part_2018'''
cursor.execute(query)
query = '''DROP TABLE users_range_part_2019'''
cursor.execute(query)
query = '''DROP TABLE users_range_part_2020'''
cursor.execute(query)

query = '''
SELECT table_catalog, 
    table_schema, 
    table_name FROM information_schema.tables
WHERE table_name ~ 'users_range_part_'
ORDER BY table_name;
'''
cursor.execute(query)
print(cursor.fetchall())

##################################PARTIONNEMENT PAR HASH##################################
query='''
DROP TABLE IF EXISTS users_hash_part
'''
cursor.execute(query)

query='''
CREATE TABLE users_hash_part (
    user_id SERIAL,
    user_first_name VARCHAR(30) NOT NULL,
    user_last_name VARCHAR(30) NOT NULL,
    user_email_id VARCHAR(50) NOT NULL,
    user_email_validated BOOLEAN DEFAULT FALSE,
    user_password VARCHAR(200),
    user_role VARCHAR(1) NOT NULL DEFAULT 'U', --U and A
    is_active BOOLEAN DEFAULT FALSE,
    created_dt DATE DEFAULT CURRENT_DATE,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
) PARTITION BY HASH(user_id)
'''
cursor.execute(query)
#partition
query='''
CREATE TABLE users_hash_part_0_of_8
PARTITION OF users_hash_part
FOR VALUES WITH (modulus 8, remainder 0)
'''
cursor.execute(query)
query='''
CREATE TABLE users_hash_part_1_of_8
PARTITION OF users_hash_part
FOR VALUES WITH (modulus 8, remainder 1)
'''
cursor.execute(query)
query='''
CREATE TABLE users_hash_part_2_of_8
PARTITION OF users_hash_part
FOR VALUES WITH (modulus 8, remainder 2)
'''
cursor.execute(query)
query='''
CREATE TABLE users_hash_part_3_of_8
PARTITION OF users_hash_part
FOR VALUES WITH (modulus 8, remainder 3)
'''
cursor.execute(query)
query='''
CREATE TABLE users_hash_part_4_of_8
PARTITION OF users_hash_part
FOR VALUES WITH (modulus 8, remainder 4)
'''
cursor.execute(query)
query='''
CREATE TABLE users_hash_part_5_of_8
PARTITION OF users_hash_part
FOR VALUES WITH (modulus 8, remainder 5)
'''
cursor.execute(query)
query='''
CREATE TABLE users_hash_part_6_of_8
PARTITION OF users_hash_part
FOR VALUES WITH (modulus 8, remainder 6)
'''
cursor.execute(query)
query='''
CREATE TABLE users_hash_part_7_of_8
PARTITION OF users_hash_part
FOR VALUES WITH (modulus 8, remainder 7)
'''
cursor.execute(query)
#insert data
query='''
INSERT INTO users_hash_part
    (user_first_name, user_last_name, user_email_id, created_dt)
VALUES 
    ('Tarek', 'Tiger', 'scott@tiger.com', '2018-10-01'),
    ('Fouad', 'Duck', 'donald@duck.com', '2019-02-10'),
    ('Sarah', 'Mouse', 'mickey@mouse.com', '2017-06-22')
'''
#selection
query='''SELECT * FROM users_hash_part
'''
cursor.execute(query)
print(cursor.fetchall())

query='''SELECT * FROM users_hash_part_0_of_8'''
cursor.execute(query)
print(cursor.fetchall())

query='''SELECT * FROM users_hash_part_1_of_8'''
cursor.execute(query)
print(cursor.fetchall())

##################################Sub Partitioning##################################
#List - Range Partitioning
query='''DROP TABLE IF EXISTS users_sub'''
cursor.execute(query)

query='''
CREATE TABLE users_sub (
    user_id SERIAL,
    user_first_name VARCHAR(30) NOT NULL,
    user_last_name VARCHAR(30) NOT NULL,
    user_email_id VARCHAR(50) NOT NULL,
    user_email_validated BOOLEAN DEFAULT FALSE,
    user_password VARCHAR(200),
    user_role VARCHAR(1) NOT NULL DEFAULT 'U', --U and A
    is_active BOOLEAN DEFAULT FALSE,
    created_dt DATE DEFAULT CURRENT_DATE,
    created_year INT,
    created_mnth INT,
    last_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (created_year, created_mnth, user_id)
) PARTITION BY LIST(created_year)'''
cursor.execute(query)

query='''
CREATE TABLE users_sub_2016
PARTITION OF users_sub
FOR VALUES IN (2016)
PARTITION BY RANGE (created_mnth)'''
cursor.execute(query)

query='''
CREATE TABLE users_sub_2016s1
PARTITION OF users_sub_2016
FOR VALUES FROM (1) TO (3)'''
cursor.execute(query)

query='''
CREATE TABLE users_sub_2016s2
PARTITION OF users_sub_2016
FOR VALUES FROM (4) TO (6)'''
cursor.execute(query)

# Make the changes to the database
connection.commit()

# Close communication with the database
cursor.close()
connection.close()