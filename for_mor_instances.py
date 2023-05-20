import os
from dotenv import load_dotenv
import mysql.connector
import psycopg2
from psycopg2 import sql
from datetime import date
from typing import Dict, Any, Tuple, Union, List

# Load environment variables from .env file
load_dotenv()


def create_config() -> Tuple[Dict[str, Union[str, int, bool, None]],
                             Dict[str, Union[str, int, None]],
                             Dict[str, Union[str, int, bool, None]]]:
    """Create MySQL and PostgreSQL configuration."""
    # MySQL SSL configuration for instance 1
    ssl_config_1 = {
        'ca': os.getenv('MYSQL_CA_1'),
        'cert': os.getenv('MYSQL_CERT_1'),
        'key': os.getenv('MYSQL_KEY_1'),
    }

    # MySQL connection configuration for instance 1
    mysql_config_1 = {
        'user': os.getenv('MYSQL_USER_1'),
        'password': os.getenv('MYSQL_PASSWORD_1'),
        'host': os.getenv('MYSQL_HOST_1'),
        'port': int(os.getenv('MYSQL_PORT_1', 0)),
        'database': os.getenv('MYSQL_DATABASE_1'),
        'ssl_ca': ssl_config_1['ca'],
        'ssl_cert': ssl_config_1['cert'],
        'ssl_key': ssl_config_1['key'],
        'ssl_verify_cert': False,  # Verify server certificate
        'ssl_disabled': False,  # Enable SSL
    }

    # MySQL SSL configuration for instance 2
    ssl_config_2 = {
        'ca': os.getenv('MYSQL_CA_2'),
        'cert': os.getenv('MYSQL_CERT_2'),
        'key': os.getenv('MYSQL_KEY_2'),
    }

    # MySQL connection configuration for instance 2
    mysql_config_2 = {
        'user': os.getenv('MYSQL_USER_2'),
        'password': os.getenv('MYSQL_PASSWORD_2'),
        'host': os.getenv('MYSQL_HOST_2'),
        'port': int(os.getenv('MYSQL_PORT_2', 0)),
        'database': os.getenv('MYSQL_DATABASE_2'),
        'ssl_ca': ssl_config_2['ca'],
        'ssl_cert': ssl_config_2['cert'],
        'ssl_key': ssl_config_2['key'],
        'ssl_verify_cert': False,  # Verify server certificate
        'ssl_disabled': False,  # Enable SSL
    }

    # PostgreSQL connection configuration
    pg_config = {
        'user': os.getenv('PG_USER'),
        'password': os.getenv('PG_PASSWORD'),
        'host': os.getenv('PG_HOST'),
        'port': int(os.getenv('PG_PORT', 0)),
        'database': os.getenv('PG_DATABASE'),
    }

    return mysql_config_1, mysql_config_2, pg_config


def main():
    """Main function to fetch data from two
    MySQL instances and insert into PostgreSQL."""
    mysql_config1, mysql_config2, pg_config = create_config()

    try:
        # Connect to the first MySQL instance and fetch data
        with mysql.connector.connect(**mysql_config1) as mysql_cnx1, mysql_cnx1.cursor() as mysql_cursor1:
            fetch_and_insert_data(mysql_cursor1, pg_config)

        # Connect to the second MySQL instance and fetch data
        with mysql.connector.connect(**mysql_config2) as mysql_cnx2, mysql_cnx2.cursor() as mysql_cursor2:
            fetch_and_insert_data(mysql_cursor2, pg_config)
    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_and_insert_data(mysql_cursor, pg_config):
    """Fetch data from a MySQL cursor and insert into PostgreSQL."""
    # Execute MySQL query
    query = "SELECT * FROM elegiveis_cv"
    mysql_cursor.execute(query)

    with psycopg2.connect(**pg_config) as pg_cnx, pg_cnx.cursor() as pg_cursor:
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

            pg_cursor.execute(sql.SQL("""
                INSERT INTO core_elegiveiscv (province,district,community,health_facility,
                patient_id, patient_name, patient_identifier, age, phone_number, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """), (province, district, community, health_facility,
                   patient_id, patient_name, patient_identifier, age, phone_number, created_at))
        pg_cnx.commit()


if __name__ == "__main__":
    main()
