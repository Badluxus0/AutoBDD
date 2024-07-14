import mysql.connector
import psycopg2
import configparser

# Lire les configurations
config = configparser.ConfigParser()
config.read('config.ini')

# Configurations MySQL
mysql_config = {
    'host': config['mysql']['host'],
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'database': config['mysql']['database']
}

# Configurations PostgreSQL
postgres_config = {
    'host': config['postgresql']['host'],
    'user': config['postgresql']['user'],
    'password': config['postgresql']['password'],
    'database': config['postgresql']['database']
}

try:
    # Connexion à MySQL
    mysql_connection = mysql.connector.connect(**mysql_config)
    cursor_mysql = mysql_connection.cursor()
except mysql.connector.Error as err:
    print(f"Erreur de connexion MySQL : {err}")
    exit(1)

try:
    # Récupérer les données de MySQL
    cursor_mysql.execute("SELECT column2 FROM table1")
    results_table1 = cursor_mysql.fetchall()

    cursor_mysql.execute("SELECT column1 FROM table2")
    results_table2 = cursor_mysql.fetchall()
except mysql.connector.Error as err:
    print(f"Erreur de lecture MySQL : {err}")
    cursor_mysql.close()
    mysql_connection.close()
    exit(1)

try:
    # Connexion à PostgreSQL
    postgres_connection = psycopg2.connect(**postgres_config)
    cursor_postgres = postgres_connection.cursor()
except psycopg2.Error as err:
    print(f"Erreur de connexion PostgreSQL : {err}")
    cursor_mysql.close()
    mysql_connection.close()
    exit(1)

try:
    # Insérer les données dans PostgreSQL
    for row1, row2 in zip(results_table1, results_table2):
        T1C2 = row1[0]
        T2C1 = row2[0]
        cursor_postgres.execute("INSERT INTO combinaisonTable (T1C2, T2C1) VALUES (%s, %s)", (T1C2, T2C1))

    # Valider et commettre les changements dans PostgreSQL
    postgres_connection.commit()
except psycopg2.Error as err:
    print(f"Erreur d'insertion PostgreSQL : {err}")
    cursor_postgres.close()
    postgres_connection.close()
    cursor_mysql.close()
    mysql_connection.close()
    exit(1)

# Message de succès
print("Les données ont été transférées avec succès.")

# Fermer les curseurs et les connexions
cursor_mysql.close()
cursor_postgres.close()
mysql_connection.close()
postgres_connection.close()
