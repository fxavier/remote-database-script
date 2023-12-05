import os
from dotenv import load_dotenv
import mysql.connector
import psycopg2
from psycopg2 import sql
from datetime import date, datetime, timedelta
from typing import Dict, Any, Tuple, Union


# Load environment variables from .env file
load_dotenv()


def create_config(province: str) -> Tuple[Dict[str, Union[str, int, bool, None]],
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
        'host': os.getenv(f'{province.upper()}_MYSQL_HOST'),
        'port': int(os.getenv(f'{province.upper()}_MYSQL_PORT', 0)),
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


def fetch_and_insert_elegiveis_cv(province: str, mysql_cursor, pg_cursor):
    """Fetch data from elegiveis_cv and insert into PostgreSQL."""
    query = "SELECT * FROM elegiveis_cv"
    mysql_cursor.execute(query)

    # Fetch all rows
    rows = mysql_cursor.fetchall()

    # Insert fetched data into PostgreSQL
    for row in rows:
        province = province
        health_facility = row[0]
        district = row[1]
        # patient_id = row[2]
        patient_identifier = row[3]
        patient_name = row[5]
        age = row[7]
        phone_number = row[8] if row[8] is not None else row[9]
        community = row[12]
        created_at = date.today()
        sent = False

        pg_cursor.execute(sql.SQL("""
            INSERT INTO core_patienteligiblevlcollection (province,district,
            community,health_facility,
            patient_name, patient_identifier, age,
            phone_number, created_at, sent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """), (province, district, community, health_facility,
               patient_name, patient_identifier, age,
               phone_number, created_at, sent))


def fetch_and_insert_carga_viral_alta(province: str, mysql_cursor, pg_cursor):
    """Fetch data from carga viral acima de 1000 and insert into PostgreSQL."""
    # Execute MySQL query
    query = "SELECT * FROM cv_acima_de_1000"
    mysql_cursor.execute(query)

    # Fetch all rows
    rows = mysql_cursor.fetchall()

    # Insert fetched data into PostgreSQL
    for row in rows:
        province = province
        district = row[1]
        health_facility = row[0]
        #   patient_id = row[0]
        patient_identifier = row[3]
        patient_name = row[4]
        # gender = row[5]
        age = row[6]
        phone_number = row[19] if row[19] is not None else row[20]
        created_at = date.today()
        sent = False

        pg_cursor.execute(sql.SQL("""
            INSERT INTO core_viralloadtestresult (
                province, district, health_facility,
                patient_name, patient_identifier,
                age, phone_number, created_at, sent
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """), (province, district, health_facility,
               patient_name, patient_identifier,
               age, phone_number, created_at, sent))


def fetch_and_insert_marcados_levantamento(province: str, mysql_cursor, pg_cursor):
    """Fetch data from marcados_para_o_levantamento 
    and insert into PostgreSQL."""

    # Check if today is Friday (4 in weekday() since Monday is 0)
    # if date.today().weekday() == 4:
    #     start_date = date.today() + timedelta(days=4)
    # else:
    #     start_date = date.today() + timedelta(days=2)

    # end_date = date.today() + timedelta(days=14)

    query = "SELECT * FROM marcados_levantamento"  # \
    #    WHERE next_dispensing_date >= %s AND next_dispensing_date <= %s"
    # mysql_cursor.execute(query, (start_date, end_date))
    mysql_cursor.execute(query)

    # Fetch all rows
    rows = mysql_cursor.fetchall()

    # Insert fetched data into PostgreSQL
    for row in rows:
        district = row[4]
        health_facility = row[1]
        patient_identifier = row[10]
        patient_name = row[9]
        gender = row[11]
        age = row[12]
        # phone_number = row[13]
        # Check if row[13] is None and assign phone_number accordingly
        phone_number = row[13] if row[13] is not None else row[14]
        appointment_date = datetime.strptime(
            str(row[2]), '%Y-%m-%d %H:%M:%S')
        next_appointment_date = datetime.strptime(
            str(row[3]), '%Y-%m-%d %H:%M:%S')
        community = row[7]
        pregnant = row[15]
        breastfeeding = row[16]
        tb = row[17]
        created_at = date.today()
        sent = False

        pg_cursor.execute(sql.SQL("""
            INSERT INTO core_visit (
                province, district, health_facility,
                patient_name, patient_identifier,
                age, phone_number, appointment_date,
                next_appointment_date, gender, community,
                pregnant, breastfeeding, tb, created_at, sent
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """), (province, district, health_facility,
               patient_name, patient_identifier,
               age, phone_number, appointment_date,
               next_appointment_date, gender, community,
               pregnant, breastfeeding, tb, created_at, sent))


def fetch_and_insert_marcados_seguimento(province: str,
                                         mysql_cursor, pg_cursor):
    """Fetch data from marcados_para_a consulta
    and insert into PostgreSQL."""

    # Check if today is Friday (4 in weekday() since Monday is 0)
    # if date.today().weekday() == 4:
    #     start_date = date.today() + timedelta(days=4)
    # else:
    #     start_date = date.today() + timedelta(days=2)

    # end_date = date.today() + timedelta(days=7)

    query = "SELECT * FROM marcados_seguimento"  # \
    #    WHERE next_appointment_date >= %s AND next_appointment_date <= %s"
    # mysql_cursor.execute(query, (start_date, end_date))
    mysql_cursor.execute(query)

    # Fetch all rows
    rows = mysql_cursor.fetchall()

    # Insert fetched data into PostgreSQL
    for row in rows:
        province = province
        district = row[4]
        health_facility = row[1]
        # patient_id = row[0]
        patient_identifier = row[10]
        patient_name = row[9]
        gender = row[11]
        age = row[12]
        # phone_number = row[13]
        # Check if row[13] is None and assign phone_number accordingly
        phone_number = row[13] if row[13] is not None else row[14]
        appointment_date = datetime.strptime(
            str(row[2]), '%Y-%m-%d %H:%M:%S')
        next_appointment_date = datetime.strptime(
            str(row[3]), '%Y-%m-%d %H:%M:%S')
        community = row[7]
        pregnant = row[15]
        breastfeeding = row[16]
        tb = row[17]
        created_at = date.today()
        sent = False

        pg_cursor.execute(sql.SQL("""
            INSERT INTO core_visit (
                province, district, health_facility,
                patient_name, patient_identifier,
                age, phone_number, appointment_date,
                next_appointment_date, gender, community,
                pregnant, breastfeeding, tb, created_at, sent
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """), (province, district, health_facility,
               patient_name, patient_identifier,
               age, phone_number, appointment_date,
               next_appointment_date, gender, community,
               pregnant, breastfeeding, tb, created_at, sent))


# def fetch_and_insert_marcados_seguimento7d(province: str,
#                                            mysql_cursor, pg_cursor):
#     """Fetch data from marcados_para_a consulta
#     and insert into PostgreSQL."""

#     next_appointment_date = date.today() + timedelta(days=9)
#     query = "SELECT * FROM marcados_seguimento \
#         WHERE next_appointment_date = %s"
#     mysql_cursor.execute(query, (next_appointment_date,))

#     # Fetch all rows
#     rows = mysql_cursor.fetchall()

#     # Insert fetched data into PostgreSQL
#     for row in rows:
#         province = province
#         district = row[4]
#         health_facility = row[1]
#         # patient_id = row[0]
#         patient_identifier = row[10]
#         patient_name = row[9]
#         gender = row[11]
#         age = row[12]
#         phone_number = row[13]
#         appointment_date = datetime.strptime(
#             str(row[2]), '%Y-%m-%d %H:%M:%S')
#         next_appointment_date = datetime.strptime(
#             str(row[3]), '%Y-%m-%d %H:%M:%S')
#         community = row[7]
#         pregnant = row[15]
#         breastfeeding = row[16]
#         tb = row[17]
#         created_at = date.today()
#         sent = False

#         pg_cursor.execute(sql.SQL("""
#             INSERT INTO core_visit (
#                 province, district, health_facility,
#                 patient_name, patient_identifier,
#                 age, phone_number, appointment_date,
#                 next_appointment_date, gender, community,
#                 pregnant, breastfeeding, tb, created_at, sent
#             )
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """), (province, district, health_facility,
#                patient_name, patient_identifier,
#                age, phone_number, appointment_date,
#                next_appointment_date, gender, community,
#                pregnant, breastfeeding, tb, created_at, sent))


def main(province: str):
    """Main function to fetch data from MySQL and insert into PostgreSQL."""
    mysql_config, pg_config = create_config(province)

    try:
        with mysql.connector.connect(**mysql_config) as mysql_cnx, \
                mysql_cnx.cursor() as mysql_cursor, \
                psycopg2.connect(**pg_config) as pg_cnx, \
                pg_cnx.cursor() as pg_cursor:

            fetch_and_insert_elegiveis_cv(province, mysql_cursor, pg_cursor)
            fetch_and_insert_carga_viral_alta(
                province, mysql_cursor, pg_cursor)
            fetch_and_insert_marcados_levantamento(
                province, mysql_cursor, pg_cursor)
            fetch_and_insert_marcados_seguimento(
                province, mysql_cursor, pg_cursor)

            pg_cnx.commit()
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Call the main function for each province
    provinces = ["Sofala", "Manica", "Niassa", "Tete"]
    for province in provinces:
        main(province)
