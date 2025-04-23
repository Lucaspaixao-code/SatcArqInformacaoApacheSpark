# Delta Lake no Apache Spark

## Modelo ER
Tabela `carro_delta`:
- id (INT)
- placa (STRING)
- marca (STRING)
- modelo (STRING)
- ano (INT)

```text
+----+--------+---------+---------+------+
| id | placa  | marca   | modelo  | ano  |
+----+--------+---------+---------+------+
```

## Fonte de Dados
Dados de exemplo (mockados): placas de veículos e informações associadas.

## Código DDL
```sql
CREATE OR REPLACE TABLE carro_delta (
  id INT, 
  placa STRING,
  marca STRING,
  modelo STRING,
  ano INT
) USING delta LOCATION 'caminho/desejado'
```

## Exemplos

### Insert
```sql
INSERT INTO carro_delta VALUES (1, 'XYZ1J34', 'Renault', 'Sandero', 2021)
```

### Update
```sql
UPDATE carro_delta SET modelo = 'Tracker', ano = 2020 WHERE id = 2
```

### Delete
```sql
DELETE FROM carro_delta WHERE id = 3
```
