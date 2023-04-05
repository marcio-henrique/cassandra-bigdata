from cassandra.cluster import Cluster
import pandas as pd

# Conecta ao cluster Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

#session.execute("DROP TABLE detran.calls")

# Cria o keyspace e a tabela
session.execute("CREATE KEYSPACE IF NOT EXISTS detran WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")

session.execute("CREATE TABLE IF NOT EXISTS detran.calls (call_id uuid, time text, location text, PRIMARY KEY (call_id))")

# # Lê o arquivo CSV e insere os dados na tabela
# with open('911.csv') as f:
#     next(f)  # pula a primeira linha do cabeçalho
#     for line in f:
#         data = line.strip().split(',')
#         call_id = data[0]
#         time = str(data[1])
#         location = str(data[4])
#         description = str(data[5])
#         session.execute(f"INSERT INTO detran.calls (call_id, time, location, description) VALUES ({call_id}, '{time}', '{location}', '{description}')")

# ler o arquivo CSV e armazená-lo em um objeto DataFrame
df = pd.read_csv('911.csv')

df['Location'] = df['Location'].str.replace("'", '')

# percorrer todas as linhas em loop
for indice, linha in df.iterrows():
    # acessar os valores da linha
    call_id = linha['SeqID']
    time = str(linha['Date Of Stop'])
    location = linha['Location']
    #description = linha['Description']
    session.execute(f"INSERT INTO detran.calls (call_id, time, location) VALUES ({call_id}, '{time}', '{location}')")

# Encerra a conexão com o cluster Cassandra
session.shutdown()
cluster.shutdown()
