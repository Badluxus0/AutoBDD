from connMySQL import mysql_connection
from connPostgreSQL import postgres_connection

# Récupérer les données de MySQL
cursor_mysql = mysql_connection.cursor()
cursor_mysql.execute("SELECT column2 FROM table1")
results_table1 = cursor_mysql.fetchall()

cursor_mysql.execute("SELECT column1 FROM table2")
results_table2 = cursor_mysql.fetchall()

# Insérer les données dans PostgreSQL
cursor_postgres = postgres_connection.cursor()
for row1, row2 in zip(results_table1, results_table2):
    T1C2 = row1[0]
    T2C1 = row2[0]
    cursor_postgres.execute("INSERT INTO combinaisonTable (T1C2, T2C1) VALUES (%s, %s)", (T1C2, T2C1))

# Valider et commettre les changements dans PostgreSQL
postgres_connection.commit()

# Fermer les curseurs et les connexions
cursor_mysql.close()
cursor_postgres.close()
mysql_connection.close()
postgres_connection.close()
