import time

import pymongo
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from faker import Faker
from mysql.connector import pooling
from pymongo import MongoClient, UpdateOne

fake = Faker()

# MySQL connection pool
mysql_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=10,
    host="localhost",
    port=3307,
    user="testuser",
    password="testpassword",
    database="testdb",
)

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
record_counts = [1000, 5000, 10000, 100000]


def create_fake_records(records):
    return [
        {"name": fake.name(), "age": fake.random_int(min=18, max=28)}
        for _ in range(records)
    ]


def create_tables_mysql():
    conn = mysql_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES LIKE 'your_table'")
    table_exists = cursor.fetchone()

    if not table_exists:
        cursor.execute(
            """
            CREATE TABLE your_table (
                field1 INT PRIMARY KEY,
                name VARCHAR(100),
                age INT
            )
        """
        )
        cursor.execute("CREATE INDEX idx_age ON your_table(age)")
        cursor.execute("CREATE INDEX idx_field ON your_table(field1)")
        print("MySQL table created successfully.")
    else:
        print("MySQL table already exists.")
    cursor.close()
    conn.close()


def create_tables_cassandra():
    if "test_keyspace" not in cassandra_cluster.metadata.keyspaces:
        cassandra_session.execute(
            """
            CREATE KEYSPACE test_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """
        )
    cassandra_session.set_keyspace("test_keyspace")
    cassandra_session.execute(
        """
        CREATE TABLE IF NOT EXISTS your_table (
            field1 INT PRIMARY KEY,
            name TEXT,
            age INT
        )
    """
    )
    cassandra_session.execute("CREATE INDEX IF NOT EXISTS idx_age ON your_table (age)")
    print("Cassandra keyspace and table created successfully.")


def create_tables_mongo():
    mongo_collection.create_index([("age", pymongo.ASCENDING)])
    mongo_collection.create_index([("field1", pymongo.ASCENDING)])
    print("MongoDB indexes created successfully.")


def create_tables_scylla():
    if "test_keyspace" not in scylla_cluster.metadata.keyspaces:
        scylla_session.execute(
            """
            CREATE KEYSPACE test_keyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """
        )
    scylla_session.set_keyspace("test_keyspace")
    scylla_session.execute(
        """
        CREATE TABLE IF NOT EXISTS your_table (
            field1 INT PRIMARY KEY,
            name TEXT,
            age INT
        )
    """
    )
    scylla_session.execute("CREATE INDEX IF NOT EXISTS idx_age ON your_table (age)")
    print("ScyllaDB keyspace and table created successfully.")


def insert_mysql(records):
    conn = mysql_pool.get_connection()
    cursor = conn.cursor()
    query = "INSERT INTO your_table (field1, name, age) VALUES (%s, %s, %s)"
    start = time.time()
    cursor.executemany(query, [(i, d["name"], d["age"]) for i, d in enumerate(data)])
    conn.commit()
    cursor.close()
    conn.close()
    return time.time() - start


def insert_mongodb(records):
    for i, d in enumerate(data):
        d["field1"] = i
    start = time.time()
    mongo_collection.insert_many(data)
    return time.time() - start


def insert_cassandra(records):
    query = "INSERT INTO your_table (field1, name, age) VALUES (?, ?, ?)"
    prepared = cassandra_session.prepare(query)
    start = time.time()
    execute_concurrent_with_args(
        cassandra_session,
        prepared,
        [(i, d["name"], d["age"]) for i, d in enumerate(data)],
        concurrency=50,
    )
    return time.time() - start


def insert_scylla(records):
    query = "INSERT INTO your_table (field1, name, age) VALUES (?, ?, ?)"
    prepared = scylla_session.prepare(query)
    start = time.time()
    execute_concurrent_with_args(
        scylla_session,
        prepared,
        [(i, d["name"], d["age"]) for i, d in enumerate(data)],
        concurrency=50,
    )
    return time.time() - start


def read_mysql(age):
    start = time.time()
    conn = mysql_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM your_table WHERE age = %s", (age,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return time.time() - start


def read_mongodb(age):
    start = time.time()
    results = mongo_collection.find({"age": age})
    return time.time() - start


def read_cassandra(age):
    start = time.time()
    prepared = cassandra_session.prepare("SELECT * FROM your_table WHERE age = ?")
    cassandra_session.execute(prepared, (age,))
    return time.time() - start


def read_scylla(age):
    start = time.time()
    prepared = scylla_session.prepare("SELECT * FROM your_table WHERE age = ?")
    scylla_session.execute(prepared, (age,))
    return time.time() - start


def update_mysql(records):
    conn = mysql_pool.get_connection()
    cursor = conn.cursor()
    query = "UPDATE your_table SET name = %s, age = %s WHERE field1 = %s"
    start = time.time()
    cursor.executemany(query, [(d["name"], d["age"], i) for i, d in enumerate(data)])
    conn.commit()
    cursor.close()
    conn.close()
    return time.time() - start


def update_mongodb(records):
    bulk_operations = []
    for i, d in enumerate(data):
        bulk_operations.append(
            UpdateOne({"field1": i}, {"$set": {"name": d["name"], "age": d["age"]}})
        )
    start = time.time()
    mongo_collection.bulk_write(bulk_operations)
    return time.time() - start


def update_cassandra(records):
    query = "UPDATE your_table SET name = ?, age = ? WHERE field1 = ?"
    prepared = cassandra_session.prepare(query)
    start = time.time()
    execute_concurrent_with_args(
        cassandra_session,
        prepared,
        [(d["name"], d["age"], i) for i, d in enumerate(data)],
        concurrency=50,
    )
    return time.time() - start


def update_scylla(records):
    query = "UPDATE your_table SET name = ?, age = ? WHERE field1 = ?"
    prepared = scylla_session.prepare(query)
    start = time.time()
    execute_concurrent_with_args(
        scylla_session,
        prepared,
        [(d["name"], d["age"], i) for i, d in enumerate(data)],
        concurrency=50,
    )
    return time.time() - start


def cleanse_mysql():
    conn = mysql_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS your_table")
    conn.commit()
    cursor.close()
    conn.close()


def cleanse_mongodb():
    mongo_db.drop_collection("testcollection")


def cleanse_cassandra():
    cassandra_session.execute("DROP KEYSPACE IF EXISTS test_keyspace")


def cleanse_scylla():
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
    data = create_fake_records(record_count)
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
mysql_pool.close()
mongo_client.close()
cassandra_cluster.shutdown()
scylla_cluster.shutdown()
