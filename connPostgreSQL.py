import psycopg2

postgres_connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="password",
    database="combinaisonDB"
)
