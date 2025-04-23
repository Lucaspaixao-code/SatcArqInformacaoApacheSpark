# Satc - Engenharia de dados com Apache Spark

Projeto de demonstração de uso de tabelas transacionais Delta Lake e Iceberg com Apache Spark.

## Requisitos

- Python 3.12+
- [Poetry](https://python-poetry.org/docs/)

## Configuração do Ambiente

```bash
# Instalar o Poetry, se ainda não tiver:
curl -sSL https://install.python-poetry.org | python3 -

# Clonar o repositório
git clone https://github.com/Lucaspaixao-code/SatcArqInformacaoApacheSpark.git
cd SatcArqInformacaoApacheSpark

# Instalar dependências
poetry install

# Ativar o ambiente virtual
poetry shell
```

## Execução

### Delta Lake

```bash
python project.py
```

### Iceberg

```bash
python project_iceberg.py
```

## Bibliotecas utilizadas

- `pyspark==3.5.3`
- `delta-spark==3.2.0`
- `mkdocs`
- `jupyterlab`
- `ipykernel`

## Documentação do Projeto

Para gerar a documentação:

```bash
mkdocs serve
```

Acesse via navegador: [http://127.0.0.1:8000](http://127.0.0.1:8000)

---

**Autores**: [Lucas Paixão](https://github.com/Lucaspaixao-code)
