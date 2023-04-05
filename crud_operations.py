from cassandra.cluster import Cluster
import pandas as pd

from cassandra.cluster import Cluster
import pandas as pd

# Conecta ao cluster Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# Consulta os dados da tabela
rows = session.execute("SELECT * FROM detran.calls LIMIT 10 ALLOW FILTERING")

print('ok')
for row in rows:
    print(row.call_id, row.time, row.location)


# Encerra a conexão com o cluster Cassandra
session.shutdown()
cluster.shutdown()