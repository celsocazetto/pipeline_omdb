# Como Executar esse projeto

[Videos do Projeto](https://www.loom.com/share/folder/065147cfa27642928bbd568087d7be2e)

# Step 1
# Preparação Docker Postgres
    1 - Execute o comando: docker run --name omdb_postgres -p 5432:5432 -e POSTGRES_USER=dbadmin -e POSTGRES_PASSWORD=dbadmin123 -e POSTGRES_DB=omdb -d postgres
    2 - Após o banco estar up conecte no banco utilizando algum SGDB de sua preferencia
    3 - Cria a estrutura do banco executando as queries do arquivo preparação_db.txt

# Step 2
# Preparação do Airflow
    1 - Abra o terminal
    2 - Navegue até a pasta do projeto em seu computador
    3 - Execute o comando: docker compose up -d


# Step 3
    1 - Acesse o Airflow web: localhost:8080
    2 - Ativa a DAG 

Pronto o projeto estará rodando. ( Se precisar trocar o filme que está sendo obtido na chamada da API basta trocar no arquivo config.json)
