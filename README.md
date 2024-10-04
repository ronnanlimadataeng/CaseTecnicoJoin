
# Case Técnico Join - Database Ecommerce

Este repositório contém a solução para um case prático utilizando o banco de dados **Ecommerce** hospedado no Azure Postgres. O objetivo é realizar um fluxo de **Engenharia de Dados** e análise de informações com ferramentas como **Databricks**, **PySpark** e **SQL**.

## Descrição Geral

O case envolve a criação de um diagrama de entidade-relacionamento (ER), a ingestão de dados no formato **.parquet** e a realização de consultas complexas para responder a perguntas de negócio. Todos os resultados são armazenados no formato **Delta**.

### Banco de Dados Utilizado

- **Host**: `{Host}`
- **Banco de Dados**: `{BancodeDados}`
- **Porta**: `{Porta}`
- **Usuário**: `{Usuário}`
- **Senha**: `{Senha}`

## Arquivos e Artefatos

- **NotebookCaseTecnicoJoin.ipynb**: Notebook contendo o fluxo de ingestão de dados com PySpark e as consultas SQL realizadas no Databricks.
- **Proposta de Diagrama ER.png**: Proposta de diagrama de entidade-relacionamento (ER) do banco de dados Ecommerce.
- **Consultas**:
  - Identificação do país com maior quantidade de itens cancelados.
  - Cálculo do faturamento da linha de produto mais vendida para pedidos realizados em 2005 com status 'Shipped'.
  - Consulta de vendedores do Japão com e-mails mascarados.
- **Resultados em Formato Delta**: Todos os resultados das consultas são salvos no formato **Delta** para garantir a integridade e versionamento dos dados.

## Ingestão de Dados

Os dados do banco de dados **Ecommerce** foram ingeridos e convertidos para o formato **.parquet** utilizando **PySpark** no Databricks. Cada tabela foi exportada como um arquivo **.parquet** separado, mantendo a estrutura original para análises posteriores.

## Diagrama Entidade-Relacionamento (ER)

Abaixo está o diagrama de entidade-relacionamento (ER) do banco de dados **Ecommerce**, que foi construído a partir da análise das tabelas. Note que, devido à falta de informações explícitas sobre chaves primárias e estrangeiras, os relacionamentos foram inferidos com base nos nomes das colunas.

![Proposta de Diagrama ER](https://github.com/ronnanlimadataeng/CaseTecnicoJoin/blob/main/Proposta%20de%20Diagrama%20ER.png)

## Consultas e Respostas

### 1. Qual país possui a maior quantidade de itens cancelados?

Esta consulta identifica o país com a maior quantidade de cancelamentos de itens, permitindo uma análise de possíveis problemas logísticos ou de comportamento do consumidor.

**Consulta SQL**:
```sql
SELECT country, COUNT(*) as cancelled_items
FROM Orders
WHERE status = 'Cancelled'
GROUP BY country
ORDER BY cancelled_items DESC
LIMIT 1;
```

### 2. Qual o faturamento da linha de produto mais vendida, considerando apenas pedidos com status 'Shipped' realizados no ano de 2005?

Aqui, é calculado o faturamento da linha de produtos mais vendida para pedidos enviados em 2005, permitindo a identificação de produtos de alto desempenho nesse período.

**Consulta SQL**:
```sql
SELECT p.product_line, SUM(od.quantity_ordered * od.price_each) as revenue
FROM Orders o
JOIN OrderDetails od ON o.order_number = od.order_number
JOIN Products p ON od.product_code = p.product_code
WHERE o.status = 'Shipped' AND YEAR(o.order_date) = 2005
GROUP BY p.product_line
ORDER BY revenue DESC
LIMIT 1;
```

### 3. Traga o nome, sobrenome e e-mail dos vendedores do Japão, com o local-part do e-mail mascarado.

Essa consulta retorna as informações dos vendedores do Japão, com a parte inicial do e-mail mascarada para garantir privacidade.

**Consulta SQL**:
```sql
SELECT CONCAT(SUBSTR(e.email, 1, 3), '****@', SUBSTRING_INDEX(e.email, '@', -1)) as masked_email,
       e.first_name, e.last_name
FROM Employees e
JOIN Offices o ON e.office_code = o.office_code
WHERE o.country = 'Japan';
```

## Salvamento dos Resultados

Os resultados das consultas foram salvos no formato **Delta** para garantir versionamento e persistência dos dados com alto desempenho em futuras consultas.

## Documentação e Processo

O processo completo da ingestão de dados, criação do diagrama ER, consultas SQL e exportação dos resultados está documentado no notebook **NotebookCaseTecnicoJoin.ipynb**. O fluxo foi realizado utilizando o Databricks Community Edition, com a integração de PySpark para ingestão e manipulação de dados.

## Como Executar

1. Crie uma conta no [Databricks Community Edition](https://community.cloud.databricks.com).
2. Faça upload do arquivo **NotebookCaseTecnicoJoin.ipynb** no ambiente Databricks.
3. Execute o notebook para realizar a ingestão dos dados e executar as consultas.
4. Consulte o diagrama ER e o resultado das queries no formato Delta.
