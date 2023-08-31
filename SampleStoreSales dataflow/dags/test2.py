import psycopg2

conn = psycopg2.connect(
    host="172.23.0.5",  # Nome do contêiner
    user="postgres",
    dbname="airflow_db",
    password="pass"
)

# Faça operações de banco de dados aqui

conn.close()