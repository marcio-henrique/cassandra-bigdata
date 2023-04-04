from cassandra.cluster import Cluster


# Conecta ao cluster Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()


# Cria o keyspace e a tabela
session.execute("CREATE KEYSPACE IF NOT EXISTS 911_calls \
                  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")


session.execute("CREATE TABLE IF NOT EXISTS 911_calls.calls \
                  (call_id int, time timestamp, location text, description text, PRIMARY KEY (call_id))")


# Lê o arquivo CSV e insere os dados na tabela
with open('911.csv') as f:
    next(f)  # pula a primeira linha do cabeçalho
    for line in f:
        data = line.strip().split(',')
        call_id = int(data[0])
        time = data[1]
        location = data[4]
        description = data[5]
        session.execute(f"INSERT INTO 911_calls.calls (call_id, time, location, description) \
                          VALUES ({call_id}, '{time}', '{location}', '{description}')")


# Consulta os dados da tabela
rows = session.execute("SELECT * FROM 911_calls.calls WHERE location = 'REAR 2131 WOODEN SHOE CT'")


for row in rows:
    print(row.call_id, row.time, row.location, row.description)


# Encerra a conexão com o cluster Cassandra
session.shutdown()
cluster.shutdown()
