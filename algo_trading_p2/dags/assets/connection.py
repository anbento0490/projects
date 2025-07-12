import logging 
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
######
class DBConnector:
    def __init__(self, user, pw, db_name):
        self.user = user
        self.pw = pw
        self.db_name = db_name
        
    def connect_to_postgres_db(self):
        try:
            # engine = create_engine(f'postgresql://{self.user}:{self.pw}@localhost:5432/{self.db_name}') #local
            engine = create_engine(f'postgresql://{self.user}:{self.pw}@postgres:5432/{self.db_name}') # docker
            logging.info(f"Connection to PostgreSQL {self.db_name} DB successful!\n\n")
            return engine
        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL DB: {e}\n")
            raise
