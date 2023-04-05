CREATE KEYSPACE IF NOT EXISTS meu_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE meu_keyspace;

CREATE TABLE IF NOT EXISTS meus_dados (
    id uuid PRIMARY KEY,
    nome text,
    idade int,
    email text
);