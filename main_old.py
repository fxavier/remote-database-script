import os
from dotenv import load_dotenv
import mysql.connector
import psycopg2
from datetime import date

# Load environment variables from .env file
load_dotenv()

# MySQL SSL configuration
ssl_config = {
    'ca': os.environ.get('MYSQL_CA'),
    'cert': os.environ.get('MYSQL_CERT'),
    'key': os.environ.get('MYSQL_KEY'),
}

# MySQL connection configuration
mysql_config = {
    'user': os.environ.get('MYSQL_USER'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'host': os.environ.get('MYSQL_HOST'),
    'port': int(os.environ.get('MYSQL_PORT', 0)),
    'database': os.environ.get('MYSQL_DATABASE'),
    'ssl_ca': ssl_config['ca'],
    'ssl_cert': ssl_config['cert'],
    'ssl_key': ssl_config['key'],
    'ssl_verify_cert': False,  # Verify server certificate
    'ssl_disabled': False,  # Enable SSL
}

# PostgreSQL connection configuration
pg_config = {
    'user': os.environ.get('PG_USER'),
    'password': os.environ.get('PG_PASSWORD'),
    'host': os.environ.get('PG_HOST'),
    'port': int(os.environ.get('PG_PORT', 0)),
    'database': os.environ.get('PG_DATABASE'),
}

# Create MySQL connection and cursor
mysql_cnx = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_cnx.cursor()

# Execute MySQL query
query = "SELECT * FROM elegiveis_cv"
mysql_cursor.execute(query)

# Create PostgreSQL connection
pg_cnx = psycopg2.connect(**pg_config)
pg_cursor = pg_cnx.cursor()

# Insert fetched data into PostgreSQL
for row in mysql_cursor:
    province = "Sofala"
    health_facility = row[0]
    district = row[1]
    patient_id = row[2]
    patient_identifier = row[3]
    patient_name = row[5]
    gender = row[6]
    age = row[7]
    phone_number = row[8]
    community = row[12]
    created_at = date.today()

    pg_cursor.execute("""
        INSERT INTO core_elegiveiscv (province,district,community,health_facility,
        patient_id, patient_name, patient_identifier, age, phone_number, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (province, district, community, health_facility,
          patient_id, patient_name, patient_identifier, age, phone_number, created_at))

# Commit the changes and close cursors and connections
pg_cnx.commit()
pg_cursor.close()
pg_cnx.close()

mysql_cursor.close()
mysql_cnx.close()
