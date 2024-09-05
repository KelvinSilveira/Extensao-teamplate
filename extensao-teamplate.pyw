import pandas as pd
from sqlalchemy import create_engine
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from io import StringIO

# Função para extrair dados do SharePoint
def extract_data_from_sharepoint(sharepoint_url, client_id, client_secret, file_path):
    """
    Função para extrair dados de um arquivo CSV do SharePoint.
    
    :param sharepoint_url: URL do site do SharePoint.
    :param client_id: ID do cliente (app).
    :param client_secret: Segredo do cliente (app).
    :param file_path: Caminho do arquivo dentro do SharePoint.
    :return: DataFrame com os dados extraídos.
    """
    # Autenticação no SharePoint
    ctx = ClientContext(sharepoint_url).with_credentials(ClientCredential(client_id, client_secret))
    
    # Acessar o arquivo no SharePoint
    response = ctx.web.get_file_by_server_relative_url(file_path).download().execute_query()
    
    # Converter o conteúdo do arquivo em um DataFrame
    file_content = StringIO(response.content.decode('utf-8'))
    df = pd.read_csv(file_content)
    
    print("Dados extraídos do SharePoint com sucesso!")
    return df

# Função para transformar dados
def transform_data(df):
    """
    Função para transformar dados.
    - Remove duplicatas
    - Calcula uma nova coluna 'total'
    """
    # Exemplo de transformação: remover duplicatas
    df = df.drop_duplicates()
    
    # Exemplo de transformação: criar uma nova coluna calculada
    if 'value' in df.columns and 'quantity' in df.columns:
        df['total'] = df['value'] * df['quantity']
    
    print("Dados transformados com sucesso!")
    return df

# Função para carregar os dados transformados em um banco de dados
def load_data(df, database_uri, table_name):
    """
    Função para carregar os dados transformados em um banco de dados.
    """
    # Conexão com o banco de dados
    engine = create_engine(database_uri)
    
    # Carregar os dados para o banco de dados
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print("Dados carregados com sucesso no banco de dados!")

# Função para executar o pipeline ETL
def run_etl_pipeline(sharepoint_url, client_id, client_secret, file_path, database_uri, table_name):
    # Extração
    data = extract_data_from_sharepoint(sharepoint_url, client_id, client_secret, file_path)
    
    # Transformação
    transformed_data = transform_data(data)
    
    # Carga
    load_data(transformed_data, database_uri, table_name)

# Configurações do pipeline
sharepoint_url = 'https://seusite.sharepoint.com'  # URL do site do SharePoint
client_id = 'seu_client_id'  # ID do cliente (app) registrado no Azure AD
client_secret = 'seu_client_secret'  # Segredo do cliente (app) registrado no Azure AD
file_path = '/sites/seusite/Shared Documents/dados.csv'  # Caminho do arquivo no SharePoint

database_uri = 'sqlite:///meu_banco.db'  # URI do banco de dados SQLite
table_name = 'dashboard_data'  # Nome da tabela no banco de dados

# Execução do pipeline ETL
run_etl_pipeline(sharepoint_url, client_id, client_secret, file_path, database_uri, table_name)
