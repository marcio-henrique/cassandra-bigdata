import uuid
from cassandra.cluster import Cluster

# Conecta ao cluster Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# Consulta os dados da tabela
rows = session.execute("SELECT * FROM detran.calls LIMIT 10 ALLOW FILTERING")

for row in rows:
    print(row.call_id, row.time, row.location)

# Cria um registro

call_id_test = uuid.uuid4()
time_test = '04/04/2023'
location_test = 'California'

print('INSERINDO REGISTRO')
session.execute(f"""INSERT INTO detran.calls (call_id, time, location) 
        VALUES ({call_id_test}, '{time_test}', '{location_test}')""")

# Consulta o registro

print('CONSULTANDO REGISTRO INSERIDO')
session.execute(f"""SELECT * FROM detran.calls WHERE (call_id = '{call_id_test}')""")


# Atualizando o registro

new_location = 'Boston'

print('ATUALIZANDO O REGISTRO')
session.execute(f"""UPDATE detran.calls SET location = '{new_location}' 
        WHERE (call_id = '{call_id_test}')""")


# Remove o registro

print('REMOVENDO REGISTRO')
session.execute(f"""DELETE FROM detran.calls WHERE (call_id = '{call_id_test}')""")

# Encerra a conexão com o cluster Cassandra
session.shutdown()
cluster.shutdown()
