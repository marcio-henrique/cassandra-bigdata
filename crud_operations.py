import uuid
from cassandra.cluster import Cluster

# Conecta ao cluster Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

print('CONSULTANDO DADOS')
# Consulta os dados da tabela
results = session.execute("SELECT * FROM detran.calls LIMIT 10 ALLOW FILTERING")
for result in results:
    print('[', result.call_id, ',', result.time, ',', result.location, ']')

# Cria um registro
call_id_test = uuid.uuid4()
time_test = '04/04/2023'
location_test = 'California'

print('\n')
print('INSERINDO REGISTRO')
session.execute(f"""INSERT INTO detran.calls (call_id, time, location) 
        VALUES ({call_id_test}, '{time_test}', '{location_test}')""")

# Consulta o registro
print('\n')
print('CONSULTANDO REGISTRO INSERIDO')
results = session.execute(f"""SELECT * FROM detran.calls WHERE (call_id = {call_id_test})""")
for result in results:
        print('[', result.call_id, ',', result.time, ',', result.location, ']') 

# Atualizando o registro

new_location = 'Boston'

print('\n')
print('ATUALIZANDO O REGISTRO')
results = session.execute(f"""UPDATE detran.calls SET location = '{new_location}' 
        WHERE (call_id = {call_id_test})""")
print(results)

# Consulta o registro
print('\n')
print('CONSULTANDO REGISTRO INSERIDO')
results = session.execute(f"""SELECT * FROM detran.calls WHERE (call_id = {call_id_test})""")
for result in results:
        print('[', result.call_id, ',', result.time, ',', result.location, ']')


# Remove o registro
print('\n')
print('REMOVENDO REGISTRO')
session.execute(f"""DELETE FROM detran.calls WHERE (call_id = {call_id_test})""")

# Encerra a conexão com o cluster Cassandra

# Consulta o registro
print('\n')
print('CONSULTANDO REGISTRO INSERIDO')
results = session.execute(f"""SELECT * FROM detran.calls WHERE (call_id = {call_id_test})""")
for result in results:
        print('[', result.call_id, ',', result.time, ',', result.location, ']')

session.shutdown()
cluster.shutdown()
