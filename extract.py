import os
import shutil
import pandas as pd
import traceback
from framework_cg.alarmistica import ProcessLogger
from framework_cg.conn import PostgresConnection
from typing import Optional

class Extract:
    def __init__(self, usuario: str = 'sistema', db_name: str | None = None):
        self.logger = ProcessLogger(usuario=usuario, db_name=db_name)
        self.db_name = db_name
        self.conn_handler = PostgresConnection()

    def mover_arquivo(self, nome_original, destino_pasta, data, origem_pasta='C:/Users/User/Downloads/csv_files'):
        """
        Procura um arquivo pelo nome (mesmo que mal codificado) na pasta de origem e o move para a pasta de destino com um novo nome.

        Parâmetros:
        origem_pasta (str): Caminho da pasta onde o arquivo original está localizado.
        nome_original (str): Nome do arquivo original (incluindo extensão, com ou sem acento).
        destino_pasta (str): Caminho da pasta onde o arquivo será movido.
        data (str): Data a ser incluída no nome do novo arquivo.

        Retorna:
        str: Caminho completo do novo arquivo ou uma mensagem de erro.
        """
        try:
            arquivos = os.listdir(origem_pasta)
            arquivo_encontrado = None
            extensoes_validas = ('.csv', '.xlsx', '.xls')
            nome_base = os.path.splitext(nome_original)[0].lower()

            for arq in arquivos:
                if os.path.splitext(arq.lower())[0] == nome_base and arq.lower().endswith(extensoes_validas):
                    arquivo_encontrado = arq
                    break

            if not arquivo_encontrado:
                return self.logger.log_mensagem(
                    f'Erro: O arquivo semelhante a "{nome_original}" não foi encontrado na pasta "{origem_pasta}"',
                    level='error'
                )

            nome_formatado = nome_original.lower().replace(" ", "_")
            nome_base_sem_extensao, extensao = os.path.splitext(nome_formatado)
            novo_nome = f"{nome_base_sem_extensao}_{data}{extensao}"

            caminho_origem = os.path.join(origem_pasta, arquivo_encontrado)
            caminho_destino = os.path.join(destino_pasta, novo_nome)

            shutil.move(caminho_origem, caminho_destino)
            return self.logger.log_mensagem(f'Arquivo movido com sucesso para: {caminho_destino}')
        
        except Exception as e:
            self.logger.log_mensagem(
                f"Erro ao mover arquivo {nome_original}: {type(e).__name__} - {e}",
                level='error'
            )
            self.logger.log_mensagem(traceback.format_exc(), level='error')
            return None
        
        
    def read_multiple_csv(self, files: dict[str, str]) -> dict[str, pd.DataFrame]:
        '''
        Lê múltiplos arquivos CSV e retorna um dicionário de DataFrames.
        '''
        dfs = {}
        for nome, arquivo in files.items():
            try:
                df = pd.read_csv(arquivo, sep=';')
                dfs[nome] = df
            except Exception as e:
                self.logger.log_mensagem(f'Erro ao processar {arquivo}: {e}', level='error')
                dfs[nome] = pd.DataFrame()
        return dfs


    def read_data_db(self, table_name: str, columns: list[str] = None, db_name: str = None, filters: dict[str, str] = None) -> pd.DataFrame:
        '''
        Executa SELECT em uma tabela do PostgreSQL com filtros opcionais.
        '''
        cols = ', '.join(columns) if columns else '*'

        where_clause = ''
        if filters:
            clauses = [f'{k} {v}' for k, v in filters.items()]
            where_clause = 'WHERE ' + ' AND '.join(clauses)

        sql = f'SELECT {cols} FROM {table_name} {where_clause}'

        with self.conn_handler.conectar_postgres(db_name=db_name) as conn:
            if conn is None:
                return pd.DataFrame()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    colnames = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(rows, columns=colnames)
            except Exception as e:
                self.logger.log_mensagem(f'Erro ao ler dados da tabela {table_name}: {type(e).__name__} - {e}', level='error')
                return pd.DataFrame()
            
    def query_data_db(
        self,
        table: str,
        columns: list[str] = None,
        db_name: str = None,
        filters: dict[str, str] = None,
        joins: list[str] = None,
        aggregations: list[str] = None,
        group_by: list[str] = None,
        order_by: str = None,
        limit: int = None,
        offset: int = None,
        custom_sql: str = None
    ) -> pd.DataFrame:
        """
        Executa uma query dinâmica no PostgreSQL com filtros, joins, agregações, group by e mais.

        Parâmetros:
            - table (str): Tabela base
            - columns (list[str]): Lista de colunas a selecionar (ignorado se houver aggregations ou custom_sql)
            - db_name (str): Nome do banco (opcional)
            - filters (dict[str, str]): Dicionário de filtros no formato {coluna: operador_expressão}, ex: {'loja': "= 'FO'"}
            - joins (list[str]): Lista de cláusulas JOIN completas
            - aggregations (list[str]): Lista de expressões agregadas (ex: ['SUM(valor) AS total'])
            - group_by (list[str]): Lista de colunas para agrupar
            - order_by (str): Expressão de ordenação (ex: 'data DESC')
            - limit (int): Limite de registros
            - offset (int): Offset para paginação
            - custom_sql (str): Query SQL completa manual (ignora todos os outros parâmetros)

        Retorna:
            pd.DataFrame: Resultado da consulta
        """
        try:
            if custom_sql:
                sql = custom_sql
            else:
                select_clause = ', '.join(aggregations or columns or ['*'])

                sql = f'SELECT {select_clause} FROM {table}'

                if joins:
                    sql += ' ' + ' '.join(joins)

                if filters:
                    where = ' AND '.join([f'{k} {v}' for k, v in filters.items()])
                    sql += f' WHERE {where}'

                if group_by:
                    sql += ' GROUP BY ' + ', '.join(group_by)

                if order_by:
                    sql += f' ORDER BY {order_by}'

                if limit:
                    sql += f' LIMIT {limit}'
                if offset:
                    sql += f' OFFSET {offset}'

            self.logger.log_mensagem(f'[DEBUG] Query executada:\n{sql}', level='info')

            with self.conn_handler.conectar_postgres(db_name=db_name) as conn:
                if conn is None:
                    return pd.DataFrame()
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    colnames = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(rows, columns=colnames)

        except Exception as e:
            self.logger.log_mensagem(
                f'Erro ao executar query na tabela {table}: {type(e).__name__} - {e}',
                level='error'
            )
            return pd.DataFrame()

