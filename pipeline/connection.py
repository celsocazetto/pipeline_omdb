from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import logging

class Connection():

    def __init__(self, usuario, senha, host, port, nome_db):
        self.usuario = usuario
        self.senha = senha
        self.host = host
        self.port = port
        self.nome_db = nome_db


    def connect(self):
        # Cria o objetivo de logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        try:
            logging.info(f'Estabelecendo conexão com o banco de dados:')        

            # Cria a URL para o banco
            DATABASE_URL = f"postgresql+psycopg2://{self.usuario}:{self.senha}@{self.host}:{self.port}/{self.nome_db}"

            # Criação do engine
            self.engine = create_engine(DATABASE_URL)

            # Criação da connexão ( para controlar o fluxo de commit e rollback quando necessário )
            self.conn = self.engine.connect()

            logging.info(f'Conexão estabelecida com sucesso.')        
        except Exception as e:
            logging.error(f'Não foi possivel estabelecer a conexão com o banco de dados: {e}')

    def close(self):        
        self.conn.commit()
        self.conn.close()
        logging.info(f'Conexão finalizada com o banco')    

    def insert_on_conflict_nothing(self, table, conn, keys, data_iter, *pk):

        try:
            logging.info(f'Iniciando o inserção na tabela: {table.table}')
            # Itera sobre as linhas para gerar um dicionario onde a key é o nome da coluna e o valor são os dados de cada linha em cada coluna.
            data = [dict(zip(keys, row)) for row in data_iter]

            # Cria o statment para caso já exista o registo não faz nada.
            stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=list(pk))

            # Executa o statement
            result = self.conn.execute(stmt)
            logging.info(f'{result.rowcount} Inseridos na tabela {table.table}')

            # Retorna a quantidade de registros afetados
            return result.rowcount
                
        except Exception as e:
            logging.error(f'Não foi possivel inserir os dados na tabela: {table} | Error: {e}')
            self.conn.rollback()
            return 0
    
    def insere_filmes(self, filmes_df, filmes_table, generos_df, genero_table, filme_genero_table, schema):
        
        # Estabelece a conexão
        self.connect()

        filmes_df.to_sql(filmes_table, self.engine, schema=schema, if_exists="append", index=False, \
                        method=  lambda table, conn, keys, data_iter: self.insert_on_conflict_nothing(table, self.conn, keys, data_iter, "id_imdb"))        

        generos_df.to_sql(genero_table, self.engine, schema=schema, if_exists="append", index=False, \
                        method=  lambda table, conn, keys, data_iter: self.insert_on_conflict_nothing(table, self.conn, keys, data_iter, "genero"))        

        genero_list = pd.read_sql(f'SELECT id id_genero FROM omdb.genero WHERE genero IN {tuple(generos_df['genero'].to_list())}', self.conn)
        genero_list['id_filme'] = filmes_df['id_imdb'][0]
        rowcount = genero_list.to_sql(filme_genero_table, self.engine, schema=schema, if_exists="append", index=False, \
                        method=  lambda table, conn, keys, data_iter: self.insert_on_conflict_nothing(table, self.conn, keys, data_iter, "id_filme", "id_genero"))

        # Fecha a conexão e efetua o commit
        self.close()
        



