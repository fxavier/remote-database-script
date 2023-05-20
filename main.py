import os
from dotenv import load_dotenv
import mysql.connector
import psycopg2
from psycopg2 import sql
from datetime import date, datetime
from typing import Dict, Any, Tuple, Union


# Load environment variables from .env file
load_dotenv()


def create_config() -> Tuple[Dict[str, Union[str, int, bool, None]],
                             Dict[str, Union[str, int, None]]]:
    """Create MySQL and PostgreSQL configuration."""
    # MySQL SSL configuration
    ssl_config = {
        'ca': os.getenv('MYSQL_CA'),
        'cert': os.getenv('MYSQL_CERT'),
        'key': os.getenv('MYSQL_KEY'),
    }

    # MySQL connection configuration
    mysql_config = {
        'user': os.getenv('MYSQL_USER'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'host': os.getenv('SOFALA_MYSQL_HOST'),
        'port': int(os.getenv('SOFALA_MYSQL_PORT', 0)),
        'database': os.getenv('MYSQL_DATABASE'),
        'ssl_ca': ssl_config['ca'],
        'ssl_cert': ssl_config['cert'],
        'ssl_key': ssl_config['key'],
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

    return mysql_config, pg_config


def fetch_and_insert_elegiveis_cv(mysql_cursor, pg_cursor):
    """Fetch data from elegiveis_cv and insert into PostgreSQL."""
    # Execute MySQL query
    query = "SELECT * FROM elegiveis_cv"
    mysql_cursor.execute(query)

    # Fetch all rows
    rows = mysql_cursor.fetchall()

    # Insert fetched data into PostgreSQL
    for row in rows:
        province = "Sofala"
        health_facility = row[0]
        district = row[1]
        # patient_id = row[2]
        patient_identifier = row[3]
        patient_name = row[5]
        age = row[7]
        phone_number = row[8]
        community = row[12]
        created_at = date.today()

        pg_cursor.execute(sql.SQL("""
            INSERT INTO core_elegiveiscv (province,district,
            community,health_facility,
            patient_name, patient_identifier, age,
            phone_number, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """), (province, district, community, health_facility,
               patient_name, patient_identifier, age,
               phone_number, created_at))


def fetch_and_insert_carga_viral_alta(mysql_cursor, pg_cursor):
    """Fetch data from carga viral acima de 1000 and insert into PostgreSQL."""
    # Execute MySQL query
    query = "SELECT * FROM cv_acima_de_1000"
    mysql_cursor.execute(query)

    # Fetch all rows
    rows = mysql_cursor.fetchall()

    # Insert fetched data into PostgreSQL
    for row in rows:
        province = "Sofala"
        district = row[1]
        health_facility = row[0]
        #   patient_id = row[0]
        patient_identifier = row[3]
        patient_name = row[4]
        # gender = row[5]
        age = row[6]
        phone_number = row[19]
        created_at = date.today()

        pg_cursor.execute(sql.SQL("""
            INSERT INTO core_pacientescargaalta (
                province, district, health_facility,
                patient_name, patient_identifier,
                age, phone_number, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """), (province, district, health_facility,
               patient_name, patient_identifier,
               age, phone_number, created_at))


def fetch_and_insert_marcados_levantamento(mysql_cursor, pg_cursor):
    """Fetch data from marcados_para_o_levantamento
    and insert into PostgreSQL."""
    # Execute MySQL query
    query = "SELECT * FROM marcados_levantamento"
    mysql_cursor.execute(query)

    # Fetch all rows
    rows = mysql_cursor.fetchall()

    # Insert fetched data into PostgreSQL
    for row in rows:
        province = "Sofala"
        district = row[1]
        health_facility = row[1]
        # patient_id = row[0]
        patient_identifier = row[10]
        patient_name = row[9]
        gender = row[11]
        age = row[12]
        phone_number = row[13]
        appointment_date = datetime.strptime(
            str(row[2]), '%Y-%m-%d %H:%M:%S')
        next_appointment_date = datetime.strptime(
            str(row[3]), '%Y-%m-%d %H:%M:%S')
        community = row[7]
        pregnant = row[15]
        breastfeeding = row[16]
        tb = row[17]
        created_at = date.today()

        pg_cursor.execute(sql.SQL("""
            INSERT INTO core_marcadoslevantamento (
                province, district, health_facility,
                patient_name, patient_identifier,
                age, phone_number, appointment_date,
                next_appointment_date, gender, community,
                pregnant, breastfeeding, tb, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """), (province, district, health_facility,
               patient_name, patient_identifier,
               age, phone_number, appointment_date,
               next_appointment_date, gender, community,
               pregnant, breastfeeding, tb, created_at))


def main():
    """Main function to fetch data from MySQL and insert into PostgreSQL."""
    mysql_config, pg_config = create_config()

    try:
        with mysql.connector.connect(**mysql_config) as mysql_cnx, \
                mysql_cnx.cursor() as mysql_cursor, \
                psycopg2.connect(**pg_config) as pg_cnx, \
                pg_cnx.cursor() as pg_cursor:

            fetch_and_insert_elegiveis_cv(mysql_cursor, pg_cursor)
            fetch_and_insert_carga_viral_alta(mysql_cursor, pg_cursor)
            fetch_and_insert_marcados_levantamento(mysql_cursor, pg_cursor)

            pg_cnx.commit()
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
