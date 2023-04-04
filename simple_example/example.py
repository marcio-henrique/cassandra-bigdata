import uuid
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = Cluster(['localhost'])
session = cluster.connect('meu_keyspace')

# Cria o keyspace e a tabela
session.execute("CREATE KEYSPACE IF NOT EXISTS meu_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}; USE meu_keyspace;")

session.execute("CREATE TABLE IF NOT EXISTS meus_dados (id uuid PRIMARY KEY, nome text, idade int, email text);")

insert_statement = session.prepare("INSERT INTO meus_dados (id, nome, idade, email) VALUES (?, ?, ?, ?)")
session.execute(insert_statement, [uuid.uuid4(), 'Alice', 25, 'alice@example.com'])
session.execute(insert_statement, [uuid.uuid4(), 'Bob', 30, 'bob@example.com'])
session.execute(insert_statement, [uuid.uuid4(), 'Charlie', 35, 'charlie@example.com'])

select_statement = SimpleStatement("SELECT * FROM meus_dados")
result_set = session.execute(select_statement)

for row in result_set:
    print(row.id, row.nome, row.idade, row.email)