import os
from typing import List, Optional
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from framework_cg.alarmistica import ProcessLogger
from framework_cg.conn import PostgresConnection


class DataLoader:
    def __init__(self, db_name: Optional[str] = None):
        self.db_name = db_name
        self.conn_handler = PostgresConnection()
        self.logger = ProcessLogger(usuario='sistema', db_name=db_name)

    @staticmethod
    def _to_native(val):
        """
        Converte tipos pandas/numpy para tipos Python nativos aceitos pelo psycopg2.
        - NaN/NaT -> None
        - numpy.* -> tipos Python
        - Timestamp -> datetime
        - Timedelta -> segundos (float)
        """
        if val is None:
            return None
        if pd.isna(val):
            return None
        if isinstance(val, (np.generic,)):
            return val.item()
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime()
        if isinstance(val, pd.Timedelta):
            return val.total_seconds()
        return val

    def transform_tuple(self, df: pd.DataFrame):
        """Transforma o DataFrame em colunas e lista de tuplas para inserção (não usada no upsert_df)."""
        cols = list(df.columns)
        tuples = [tuple(self._to_native(v) for v in row) for row in df.itertuples(index=False, name=None)]
        return cols, tuples

    @staticmethod
    def _qual_name(table: str) -> sql.SQL:
        """
        Constrói nome qualificado da tabela com quoting seguro.
        Aceita 'tabela' ou 'schema.tabela'.
        """
        parts = [p.strip() for p in table.split('.')]
        idents = [sql.Identifier(p) for p in parts]
        return sql.SQL('.').join(idents)

    @staticmethod
    def _base_name(table: str) -> sql.Identifier:
        """Retorna somente o último identificador (nome da tabela sem schema)."""
        return sql.Identifier(table.split('.')[-1].strip())
    @staticmethod
    def _qualify_table(table: str) -> sql.Composed:
        """
        Converte 'schema.tabela' ou 'tabela' em um identificador seguro para SQL.
        """
        if '.' in table:
            schema, tbl = table.split('.', 1)
            return sql.SQL('.').join([sql.Identifier(schema), sql.Identifier(tbl)])
        return sql.Identifier(table)

    def simple_insert(self, table: str, df: pd.DataFrame, page_size: int = 1000) -> int:
        """
        INSERT em lote usando VALUES %s.
        - Converte NaN -> None
        - Usa quoting seguro para tabela/colunas
        - Faz commit explícito
        Retorna quantidade de linhas inseridas (estimada pelo tamanho do `data`).
        """
        if df is None or df.empty:
            self.logger.log_mensagem(f"DataFrame vazio para {table}.", level='warning')
            return 0

        # Ordem determinística das colunas
        insert_cols = list(df.columns)

        # Evita NaN/NaT no psycopg2
        df2 = df.where(pd.notnull(df), None)

        data = [tuple(self._to_native(v) for v in row)
                for row in df2[insert_cols].itertuples(index=False, name=None)]

        tbl = self._qualify_table(table)
        cols = sql.SQL(', ').join(sql.Identifier(c) for c in insert_cols)
        q = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(tbl, cols)

        try:
            with self.conn_handler.conectar_postgres(db_name=self.db_name) as conn:
                if conn is None:
                    self.logger.log_mensagem("Falha ao conectar ao banco.", level='error')
                    return 0
                with conn.cursor() as cur:
                    # `execute_values` precisa de string; usamos as_string com o cursor
                    query_str = q.as_string(cur)
                    execute_values(cur, query_str, data, page_size=page_size)
                conn.commit()

            self.logger.log_mensagem(f"{len(data)} linhas inseridas em {table}.", level='info')
            return len(data)

        except psycopg2.Error as e:
            code = getattr(e, "pgcode", None)
            pgerr = getattr(e, "pgerror", None) or str(e)
            self.logger.log_mensagem(
                f"Erro no insert em {table}: {type(e).__name__} (code={code}) {pgerr}",
                level='error'
            )
            return 0

    def upsert_df(
        self,
        table: str,
        df: pd.DataFrame,
        conflict_cols: List[str],                   
        exclude_update: Optional[List[str]] = None, 
        on_conflict: str = 'update',              
        chunk_size: int = 10000
    ) -> None:
        """
        UPSERT genérico com execute_values:
        - Usa conflict_cols como alvo do ON CONFLICT.
        - Atualiza todas as colunas do DF, exceto conflict_cols e exclude_update.
        - Evita 'updates no-op' com WHERE IS DISTINCT FROM.
        """
        if df is None or df.empty:
            self.logger.log_mensagem(f"DataFrame vazio para {table}.", level='warning')
            return

        conflict_cols = list(conflict_cols or [])
        if not conflict_cols:
            raise ValueError("conflict_cols não pode ser vazio para upsert.")

        exclude_update = set(exclude_update or [])

        insert_cols = list(df.columns)  # ordem determinística
        data = [tuple(self._to_native(v) for v in row)
                for row in df[insert_cols].itertuples(index=False, name=None)]

        tbl_qual = self._qual_name(table)
        tbl_base = self._base_name(table)
        cols_ident = [sql.Identifier(c) for c in insert_cols]
        cols_str = sql.SQL(", ").join(cols_ident)

        upd_cols = [c for c in insert_cols if c not in (set(conflict_cols) | exclude_update)]

        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(tbl_qual, cols_str)

        conflict_ident = sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols)

        if on_conflict == "nothing" or not upd_cols:
            on_conflict_sql = sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(conflict_ident)
        elif on_conflict == "update":
            set_pairs = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in upd_cols
            )

            where_diff = sql.SQL(" OR ").join(
                sql.SQL("EXCLUDED.{c} IS DISTINCT FROM {t}.{c}")
                .format(c=sql.Identifier(c), t=tbl_base)
                for c in upd_cols
            )
            on_conflict_sql = (
                sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {} WHERE {}")
                .format(conflict_ident, set_pairs, where_diff)
            )
        else:
            raise ValueError("on_conflict deve ser 'update' ou 'nothing'.")

        full_sql = insert_sql + on_conflict_sql

        try:
            with self.conn_handler.conectar_postgres(db_name=self.db_name) as conn:
                if conn is None:
                    self.logger.log_mensagem("Falha ao conectar ao banco.", level='error')
                    return
                with conn.cursor() as cur:
                    for i in range(0, len(data), chunk_size):
                        chunk = data[i:i + chunk_size]
                        execute_values(cur, full_sql.as_string(conn), chunk, page_size=len(chunk))
                conn.commit()
            self.logger.log_mensagem(f"{len(data)} linhas processadas em {table}.", level='info')

        except psycopg2.Error as e:
            code = getattr(e, "pgcode", None)
            pgerr = getattr(e, "pgerror", None) or str(e)
            self.logger.log_mensagem(
                f"Erro no upsert em {table}: {type(e).__name__} (code={code}) {pgerr}",
                level='error'
            )
