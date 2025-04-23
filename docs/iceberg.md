# Apache Iceberg no Apache Spark

## Modelo ER
Tabela `medico_iceberg`:
- id (INT)
- nome (STRING)
- registro (INT)
- especialidade (STRING)
- telefone (STRING)

```text
+----+---------+-----------+----------------+---------------+
| id | nome    | registro  | especialidade  | telefone      |
+----+---------+-----------+----------------+---------------+
```

## Fonte de Dados
Dados simulados de médicos e suas especialidades.

## Código DDL
```sql
CREATE TABLE local.medico_iceberg (
  id INT,
  nome STRING,
  registro INT,
  especialidade STRING,
  telefone STRING
) USING iceberg
```

## Exemplos

### Insert
```sql
INSERT INTO local.medico_iceberg VALUES (1, 'William', 20205178, 'Cardiologista', '48 99632-1234')
```

### Update
```sql
UPDATE local.medico_iceberg SET especialidade = 'Ortopedista' WHERE id = 2
```

### Delete
```sql
DELETE FROM local.medico_iceberg WHERE id = 3
```
