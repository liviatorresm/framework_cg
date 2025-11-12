import os
from typing import Dict, Optional, Iterator
from contextlib import contextmanager

from dotenv import load_dotenv
from psycopg2 import connect, Error as PsycopgError

from framework_cg.alarmistica import ProcessLogger

load_dotenv('.env')

logger = ProcessLogger(usuario='sistema', db_name=os.getenv('DB_NAME'))


class PostgresConnection:
    def __init__(self) -> None:
        self.logger = logger

    def carregar_variaveis_ambiente(self, db_name: Optional[str] = None) -> Dict[str, str]:
        dsn = os.getenv('DATABASE_URL')  
        if dsn:
            return {"dsn": dsn}

        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        name = db_name or os.getenv('DB_NAME')

        return {
            "host": host,
            "port": int(port) if port else None,
            "user": user,
            "password": password,
            "dbname": name,
            "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
            "keepalives": 1,
            "keepalives_idle": int(os.getenv("DB_KEEPALIVE_IDLE", "30")),
            "keepalives_interval": int(os.getenv("DB_KEEPALIVE_INTERVAL", "10")),
            "keepalives_count": int(os.getenv("DB_KEEPALIVE_COUNT", "5")),
            "sslmode": os.getenv("DB_SSLMODE", None) or None, 
        }


    @contextmanager
    def conectar_postgres(self, db_name: str = None):
        params = self.carregar_variaveis_ambiente(db_name)
        conn = None
        try:
            conn = connect(**params)
            self.logger.log_mensagem('Conexão com PostgreSQL estabelecida com sucesso.', level='info')
            yield conn
        except Exception as e:
            self.logger.log_mensagem(f'Erro ao conectar/operar no PostgreSQL: {type(e).__name__} - {e}', level='error')
            raise 
        finally:
            if conn and not conn.closed:
                conn.close()
                self.logger.log_mensagem('Conexão com PostgreSQL fechada com sucesso.', level='info')
