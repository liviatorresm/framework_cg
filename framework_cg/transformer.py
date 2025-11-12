import pandas as pd
import unidecode
import hashlib
import inspect


class Transformer:
    def __init__(self, modulo_funcoes=None):
        self.modulo_funcoes = modulo_funcoes

    @staticmethod
    def limpar_colunas(df: pd.DataFrame) -> pd.DataFrame:
        '''
        Limpa os nomes das colunas do DataFrame: remove acentos, espaços, pontos, deixa tudo minúsculo.
        '''
        df.columns = [
            unidecode.unidecode(col).strip().lower().replace(" ", "_").replace('.', '')
            for col in df.columns
        ]
        return df

    @staticmethod
    def gerar_hash_linha(row: pd.Series, key_columns = list[str]) -> str:
        '''
        Gera um hash MD5 único para uma linha do DataFrame.
        '''
        valores = [str(row[col]) for col in key_columns]
        linha_concatenada = ';'.join(valores)
        return hashlib.md5(linha_concatenada.encode('utf-8')).hexdigest()

    @staticmethod
    def limpar_texto(texto) -> str:
        '''
        Remove acentos e transforma texto em minúsculo. Retorna string vazia se for nulo.
        '''
        if pd.isna(texto):
            return ''
        return unidecode.unidecode(str(texto)).lower()


    def aplicar(self, df: pd.DataFrame, funcoes: list[str], **kwargs) -> pd.DataFrame:
        '''
        Aplica funções do módulo externo ao DataFrame, na ordem especificada.
        Suporta funções com múltiplos argumentos, desde que os nomes estejam em kwargs.
        '''
        for nome_funcao in funcoes:
            if not hasattr(self.modulo_funcoes, nome_funcao):
                raise AttributeError(f'A função "{nome_funcao}" não foi encontrada no módulo.')

            func = getattr(self.modulo_funcoes, nome_funcao)
            sig = inspect.signature(func)
            params = sig.parameters

            args = {}
            for nome_param in params:
                if nome_param == 'df':
                    args[nome_param] = df
                elif nome_param in kwargs:
                    args[nome_param] = kwargs[nome_param]
                elif params[nome_param].default is inspect.Parameter.empty:
                    raise ValueError(f'O parâmetro obrigatório "{nome_param}" não foi passado para a função "{nome_funcao}".')

            df = func(**args)
        return df

