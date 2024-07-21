import psycopg2

class DatabaseConnector:
    def __init__(self):
        self.conn = psycopg2.connect("this is my server")
        self.cursor = self.conn.cursor()

    def execute_query(self, query, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        self.conn.commit()

    def executemany_query(self, query, params=None):
        if params:
            self.cursor.executemany(query, params)
        else:
            self.cursor.execute(query)
        self.conn.commit()
    
    def fetch_data(self, query, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()

    def close_connection(self):
        self.cursor.close()
        self.conn.close()