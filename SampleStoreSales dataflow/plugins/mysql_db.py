import mysql.connector

class MySqlDB():

    def __init__(self, host, user, password, database) -> None:    
        self.db = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.db.cursor()

    def drop_table(self, table_name):
        query = f"DROP TABLE IF EXISTS {table_name}"
        self.cursor.execute(query)
    
    def create_table(self, table_name, **kwargs):      
        self.drop_table(table_name)  
        query = f"CREATE TABLE {table_name} ("
        for k, v in kwargs.items():
            query += f"{k} {v}, "
        query = query[:-2]
        query += ")"
        print(query)
        self.cursor.execute(query)

    def insert_data(self, table_name, data):
        data.columns = data.columns.str.replace(' ', '_')
        query = f"INSERT INTO {table_name} ("
        query += ", ".join(data.columns)
        query += ") VALUES ("
        for _, row in data.iterrows():         
            values = ", ".join([str(x) for x in row])
            ins = query + values + ")"
            self.cursor.execute(ins)


























"""

if __name__ == "__main__":
    db = MySqlDB('localhost', 'root', 'Lotropika146214', 'airflow_db')
    #db.cursor.execute("SHOW DATABASES")
    #for c in db.cursor:
    #    print(c)

    dic = {
        'nome': 'VARCHAR(255)',
        'genero': 'VARCHAR(255)'
    }
    db.create_table('filmes', **dic)
    db.cursor.execute("SHOW TABLES")
    for c in db.cursor:
        print(c)

"""