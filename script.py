import time
import mysql.connector
from pymongo import MongoClient
from pymongo import UpdateOne
import pymongo
from cassandra.cluster import Cluster
from faker import Faker
from cassandra.query import BatchStatement, SimpleStatement

fake = Faker()
def create_fake_records(records):
    return [{"name": fake.name(), "age": fake.random_int(min=18, max=28)} for _ in range(records)]

# MySQL connection
mysql_conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    user="testuser",
    password="testpassword",
    database="testdb"
)
mysql_cursor = mysql_conn.cursor()

# MongoDB setup
mongo_client = MongoClient("mongodb://localhost:27018/")
mongo_db = mongo_client["testdb"]
mongo_collection = mongo_db["testcollection"]

# Cassandra setup
cassandra_cluster = Cluster(["localhost"])
cassandra_session = cassandra_cluster.connect()

# ScyllaDB setup
scylla_cluster = Cluster(["localhost"], port=9043)
scylla_session = scylla_cluster.connect()

# Number of records to test with
record_counts = [1000, 5000, 10000]

def create_tables_mysql():
    # Check if table exists
    mysql_cursor.execute("SHOW TABLES LIKE 'your_table'")
    table_exists = mysql_cursor.fetchone()
    
    if not table_exists:
        # Create table in MySQL
        mysql_cursor.execute("""
            CREATE TABLE your_table (
                field1 INT PRIMARY KEY,
                name VARCHAR(100),
                age INT
            )
        """)

        mysql_cursor.execute("CREATE INDEX idx_age ON your_table(age)")
        mysql_cursor.execute("CREATE INDEX idx_field ON your_table(field1)")
        
        print("MySQL table created successfully.")
    else:
        print("MySQL table already exists.")

def create_tables_cassandra():
    # Check if keyspace exists
    existing_keyspaces = cassandra_cluster.metadata.keyspaces
    if 'test_keyspace' not in existing_keyspaces:
        # Create keyspace
        cassandra_session.execute("""
            CREATE KEYSPACE test_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)
        
        # Use the keyspace
        cassandra_session.set_keyspace('test_keyspace')
        
        # Create table
        cassandra_session.execute("""
            CREATE TABLE your_table (
                field1 INT PRIMARY KEY,
                name TEXT,
                age INT
            )
        """)
        cassandra_session.execute("CREATE INDEX IF NOT EXISTS idx_age ON your_table (age);")
        
        print("Cassandra tables created successfully.")
    else:
        print("Cassandra keyspace and table already exist.")

def create_tables_mongo():
    print("Mongo indexes created successfully")
    mongo_collection.create_index([("age", pymongo.ASCENDING)])
    mongo_collection.create_index([("field1", pymongo.ASCENDING)])

def create_tables_scylla():
    # Check if keyspace exists
    existing_keyspaces = scylla_cluster.metadata.keyspaces
    if 'test_keyspace' not in existing_keyspaces:
        # Create keyspace
        scylla_session.execute("""
            CREATE KEYSPACE test_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        
        # Use the keyspace
        scylla_session.set_keyspace('test_keyspace')
        
        # Create table
        scylla_session.execute("""
            CREATE TABLE your_table (
                field1 INT PRIMARY KEY,
                name TEXT,
                age INT
            )
        """)
        scylla_session.execute("CREATE INDEX IF NOT EXISTS idx_age ON your_table (age);")
        
        print("ScyllaDB tables created successfully.")
    else:
        print("ScyllaDB keyspace and table already exist.")

def insert_mysql(records):
    start = time.time()
    data = create_fake_records(records)
    query = "INSERT INTO your_table (field1, name, age) VALUES (%s, %s, %s)"
    mysql_cursor.executemany(query, [(i, d['name'], d['age']) for i, d in enumerate(data)])
    mysql_conn.commit()
    return time.time() - start

def insert_mongodb(records):
    start = time.time()
    data = create_fake_records(records)
    for i, d in enumerate(data):
        d["field1"] = i
    mongo_collection.insert_many(data)
    return time.time() - start

def insert_cassandra(records):
    start = time.time()
    data = create_fake_records(records)
    query = "INSERT INTO your_table (field1, name, age) VALUES (%s, %s, %s)"
    batch = BatchStatement()
    for i, d in enumerate(data):
        batch.add(SimpleStatement(query), (i, d['name'], d['age']))
        if i % 500 == 0:
          cassandra_session.execute(batch)
          batch = BatchStatement()
    cassandra_session.execute(batch)
    return time.time() - start

def insert_scylla(records):
    start = time.time()
    data = create_fake_records(records)
    query = "INSERT INTO your_table (field1, name, age) VALUES (%s, %s, %s)"
    batch = BatchStatement()
    for i, d in enumerate(data):
        batch.add(SimpleStatement(query), (i, d['name'], d['age']))
        if i % 500 == 0:
          cassandra_session.execute(batch)
          batch = BatchStatement()
    scylla_session.execute(batch)
    return time.time() - start

def read_mysql(age):
    start = time.time()
    mysql_cursor.execute("SELECT * FROM your_table WHERE age = %s", (age,))
    result = mysql_cursor.fetchall()
    return time.time() - start

def read_mongodb(age):
    start = time.time()
    results = mongo_collection.find({"age": age})
    return time.time() - start

def read_cassandra(age):
    start = time.time()
    cassandra_session.execute("SELECT * FROM your_table WHERE age = %s", (age,))
    return time.time() - start

def read_scylla(age):
    start = time.time()
    scylla_session.execute("SELECT * FROM your_table WHERE age = %s", (age,))
    return time.time() - start

def update_mysql(records):
    start = time.time()
    data = create_fake_records(records)
    query = "UPDATE your_table SET name = %s, age = %s WHERE field1 = %s"
    mysql_cursor.executemany(query, [(d['name'], d['age'], i) for i, d in enumerate(data)])
    mysql_conn.commit()
    return time.time() - start

def update_mongodb(records):
    start = time.time()
    data = create_fake_records(records)
    
    bulk_operations = []
    for i, d in enumerate(data):
        bulk_operations.append(
            UpdateOne({"field1": i}, {"$set": {"name": d['name'], "age": d['age']}})
        )
    
    mongo_collection.bulk_write(bulk_operations)
    return time.time() - start

def update_cassandra(records):
    start = time.time()
    data = create_fake_records(records)
    query = "UPDATE your_table SET name = %s, age = %s WHERE field1 = %s"
    batch = BatchStatement()
    for i, d in enumerate(data):
        batch.add(SimpleStatement(query), (d['name'], d['age'], i))
        if i % 500 == 0:
            cassandra_session.execute(batch)
            batch = BatchStatement()
    cassandra_session.execute(batch)
    return time.time() - start

def update_scylla(records):
    start = time.time()
    data = create_fake_records(records)
    query = "UPDATE your_table SET name = %s, age = %s WHERE field1 = %s"
    batch = BatchStatement()
    for i, d in enumerate(data):
        batch.add(SimpleStatement(query), (d['name'], d['age'], i))
        if i % 500 == 0:
            scylla_session.execute(batch)
            batch = BatchStatement()
    scylla_session.execute(batch)
    return time.time() - start

def cleanse_mysql():
    # Drop table if exists to cleanse
    mysql_cursor.execute("DROP TABLE IF EXISTS your_table")
    mysql_conn.commit()

def cleanse_mongodb():
    # Drop collection to cleanse
    mongo_db.drop_collection("testcollection")

def cleanse_cassandra():
    # Drop keyspace to cleanse
    cassandra_session.execute("DROP KEYSPACE IF EXISTS test_keyspace")

def cleanse_scylla():
    # Drop keyspace to cleanse
    scylla_session.execute("DROP KEYSPACE IF EXISTS test_keyspace")

def cleanse_databases():
    print("[Database Deletion] ---")
    cleanse_mysql()
    cleanse_mongodb()
    cleanse_cassandra()
    cleanse_scylla()

def create_databases():
    print("[Database Creation] --- ")
    create_tables_mongo()
    create_tables_mysql()
    create_tables_cassandra()
    create_tables_scylla() 

cleanse_databases()

# Test operations
for record_count in record_counts:
    create_databases()

    print(f"\nTesting with {record_count} records ----------------------")

    print("\nInsert speeds")
    print(f"MySQL: {insert_mysql(record_count)} seconds")
    print(f"MongoDB: {insert_mongodb(record_count)} seconds")
    print(f"Cassandra: {insert_cassandra(record_count)} seconds")
    print(f"ScyllaDB: {insert_scylla(record_count)} seconds")

    age = fake.random_int(min=18, max=28)

    print("\nRead speeds")
    print(f"MySQL: {read_mysql(age)} seconds")
    print(f"MongoDB: {read_mongodb(age)} seconds")
    print(f"Cassandra: {read_cassandra(age)} seconds")
    print(f"ScyllaDB: {read_scylla(age)} seconds")

    print("\nUpdate speeds")
    print(f"MySQL: {update_mysql(record_count)} seconds")
    print(f"MongoDB: {update_mongodb(record_count)} seconds")
    print(f"Cassandra: {update_cassandra(record_count)} seconds")
    print(f"ScyllaDB: {update_scylla(record_count)} seconds")

    print("-----------------------------")
    
    # Cleanse databases after each test iteration
    cleanse_databases()

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
cassandra_cluster.shutdown()
scylla_cluster.shutdown()
