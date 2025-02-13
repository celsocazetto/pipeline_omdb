# Importa os pacotes necessários
import requests
import json
import pandas as pd
import sqlalchemy
from pandas import json_normalize
from dateutil.relativedelta import relativedelta
from datetime import date
from connection import Connection
import logging
import sys
import os
from pathlib import Path

# Cria o objeto de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Obter o diretório do script atual
script_dir = Path(__file__).parent
config_path = script_dir.parent / 'pipeline' / 'config.json'

# Carrega o arquivo de configuração
with open(config_path, 'r') as file:
    logging.info('Carregando o arquivo de configuração')
    config = json.load(file)

# Faz a chamada a API para retornar o ID do file do config file.
logging.info(f'Buscando os dados de filme do ID:{config['filmeid']}')
response = requests.get(f'http://www.omdbapi.com/?apikey={config['apikey']}&i={config['filmeid']}')

# Obtem os dados da resposta
if response.status_code != 200:
    logging.info(f'Não foi possivel carregar os dados da API')
    sys.exit()

filme = response.json()
logging.info(f'Dados da API carregados com sucesso')

# Configura os nome de colunas que serão extraidos do json de acordo com as tabelas
logging.info(f'Iniciando transformação dos dados.')
filme_columns = ['imdbID', 'Title', 'Released', 'Director', 'Plot']
genero_column = 'Genre'

# Converte o json para um dataframe do pandas
filme_df = json_normalize(filme)
# Cria um dataframe com os generos do filme
genero_df = pd.melt(filme_df[genero_column].str.split(',', expand=True), value_name='genero').drop(columns='variable')
# Cria um dataframe com as informações do filme
filme_df = filme_df[filme_columns]

# Faz o ajuste dos nomes das colunas
filme_df.rename(columns={
    'imdbID':'id_imdb',
    'Title':'titulo',
    'Released':'ano_lancamento',
    'Director':'diretor',
    'Plot':'sinopse'
}, inplace=True)

# Faz a limpeza e tratamento nos dados de filme.
filme_df['titulo'] = filme_df['titulo'].str.title()
filme_df['titulo'] = filme_df['titulo'].str.strip()
filme_df['diretor'] = filme_df['diretor'].str.title()
filme_df['diretor'] = filme_df['diretor'].str.strip()
filme_df['ano_lancamento'] = pd.to_datetime(filme_df['ano_lancamento'])         # --> Por ser data apenas o output já é no formato ISO8601 Ex: 2025-02-12
filme_df['dias'] = (date.today() - filme_df['ano_lancamento'].dt.date[0]).days
diferenca = relativedelta(date.today(), filme_df['ano_lancamento'].dt.date[0])
filme_df['meses'] = diferenca.years * 12 + diferenca.months
filme_df['anos'] = diferenca.years

# Faz a limpeza e tratamento no dados de genero
genero_df['genero'] = genero_df['genero'].str.strip() # Remove os espaços em branco
genero_df['genero'] = genero_df['genero'].str.title() # Garante a correta capitalização onde cada palavra começa com a letra maiuscula

# Cria o objeto de conexão para eftuar a criação dos dados no banco.
conn = Connection(config['database']['usuario'], \
                  config['database']['senha'], \
                  config['database']['host'], \
                  config['database']['port'], \
                  config['database']['nome_db'])

# Executa a ingestão dos dados no banco
conn.insere_filmes(filme_df, 'filmes', genero_df, 'genero', 'filme_genero', 'omdb')

logging.info('Fim do processo de ingestão.')

