import psycopg2
from psycopg2 import sql

admin_connection = {
    "dbname": "postgres",
    "user": "jakeg",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

db_name = "zillow_analytics"
db_user = "zillow_user"
db_password = "zillow_password"

try:
    with psycopg2.connect(**admin_connection) as conn:
        conn.autocommit = True  # Ensure commands run outside transactions
        with conn.cursor() as curs:
            # Drop the database and user
            curs.execute(sql.SQL("DROP DATABASE IF EXISTS {};").format(sql.Identifier(db_name)))
            curs.execute(sql.SQL("DROP USER IF EXISTS {};").format(sql.Identifier(db_user)))

except Exception as e:
    print(f"An error occurred: {e}")


try:
    # Connect to PostgreSQL as superuser
    with psycopg2.connect(**admin_connection) as conn:
        with conn.cursor() as curs:

            # Create the database
            curs.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(db_name)))
            conn.autocommit = True  # Ensure commands run outside transactions
            # Create the user and grant privileges
            curs.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s;").format(sql.Identifier(db_user)), [db_password])
            curs.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {};").format(
                sql.Identifier(db_name),
                sql.Identifier(db_user)
            ))

    print("Database and user setup complete!")

except Exception as e:
    print(f"An error occurred: {e}")

