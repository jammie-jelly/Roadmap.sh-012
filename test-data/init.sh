#!/bin/bash
set -e

echo "Starting DB services..."

## DB format ->  db-name:user:pass

# postgres:postgres:""
echo "listen_addresses = '*'" >> /etc/postgresql/16/main/postgresql.conf
echo "host all all all trust" >> /etc/postgresql/16/main/pg_hba.conf
service postgresql start

# testdb:root:""
mkdir -p /var/lib/mysql
chown mysql:mysql /var/lib/mysql
usermod -d /var/lib/mysql mysql
sed -i 's/^bind-address\s*=.*/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf || true
service mysql start
sleep 2
mysql -u root -e "CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '';"
mysql -u root -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;"
mysql -u root -e "FLUSH PRIVILEGES;"

# test:"":""
mongod --bind_ip 0.0.0.0 --dbpath /var/lib/mongodb --logpath /var/log/mongodb/mongod.log --fork
sleep 1

echo "Generating data for MySQL & Postgres..."

python3 - <<EOF
import json
from faker import Faker

fake = Faker()
N = 6000

rows = []
mongo_docs = []

for _ in range(N):
    name = fake.name()
    email = fake.email()
    safe_name = name.replace("'", "''")  
    rows.append(f"('{safe_name}', '{email}')")
    mongo_docs.append({
        "name": name,
        "email": email,
        "address": fake.address().replace('\n', ', '),
        "phone": fake.phone_number(),
        "company": fake.company()
    })

# PostgreSQL
with open("/tmp/postgres.sql", "w") as f:
    f.write("DROP TABLE IF EXISTS users;\n")
    f.write("""CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100)
    );\n""")
    f.write("INSERT INTO users (name, email) VALUES\n")
    f.write(",\n".join(rows) + ";\n")

# MySQL
with open("/tmp/mysql.sql", "w") as f:
    f.write("DROP DATABASE IF EXISTS testdb;\n")
    f.write("CREATE DATABASE testdb;\nUSE testdb;\n")
    f.write("""CREATE TABLE users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100)
    );\n""")
    f.write("INSERT INTO users (name, email) VALUES\n")
    f.write(",\n".join(rows) + ";\n")

# MongoDB
with open("/tmp/mongo.json", "w") as f:
    json.dump(mongo_docs, f)
EOF

echo "Creating SQLite DB data..."

SQLITE_DB="/data/sample.sqlite"
rm -f "$SQLITE_DB"

sqlite3 "$SQLITE_DB" <<EOF
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    email TEXT
);
EOF

python3 - <<EOF
from faker import Faker
import sqlite3

fake = Faker()
conn = sqlite3.connect("$SQLITE_DB")
c = conn.cursor()

for _ in range(6000):
    name = fake.name()
    email = fake.email()
    c.execute("INSERT INTO users (name, email) VALUES (?, ?)", (name, email))

conn.commit()
conn.close()
EOF

runuser postgres -c "psql -qAt -c 'SELECT 1'" > /dev/null || { echo 'PostgreSQL failed to start'; exit 1; }
echo "Loading data into PostgreSQL..."
runuser postgres -c "psql -f /tmp/postgres.sql"
sleep 1

mysqladmin ping -u root || { echo "MySQL failed to start"; exit 1; }
echo "Loading data into MySQL..."
mysql -u root < /tmp/mysql.sql
sleep 1

echo "Checking MongoDB connection..."
mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1 || { echo "Mongo failed to start"; exit 1; }
echo "Importing MongoDB data..."
mongoimport --db test --collection users --file /tmp/mongo.json --jsonArray

echo "✔️ All databases ready with dummy data"

tail -f /dev/null
