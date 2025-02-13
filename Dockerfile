FROM apache/airflow
COPY requirements.txt .

# Crie um diretório para o ambiente virtual
RUN mkdir -p /home/airflow/venv

# Crie o ambiente virtual
RUN python3 -m venv /home/airflow/venv/omdb_env

# Ative o ambiente virtual e instale as dependências
RUN /home/airflow/venv/omdb_env/bin/pip install --upgrade pip    
RUN /home/airflow/venv/omdb_env/bin/pip install -r requirements.txt
