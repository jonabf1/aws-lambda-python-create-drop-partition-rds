Classe principal: ~/src/handlers/lambda_handler.py 

Variaveis ambientes:
- application_name (nome do projeto, reflete no nome do grupo de logs do cloudwatch)
- enable_drop (ativa/desativa fluxo de drop partition)
- enable_create (ativa/desativa fluxo de criacao de partition)
- host (host do banco SQL)
- user (user do banco SQL)
- password (password do banco SQL)
- database_name (database do banco SQL)
- table_name (table do banco SQL)
- future_partition_months (numero de partições mensais futuras a serem criadas)
- drop_month_back (valor a ser usado como referencia para drop remanescente de partições mensais.)
  - ex: Se hoje é 25/10/2024 e o valor utilizado for 2, irá excluir a partição do mes 08
- maxvalue_partition_name (o nome da particao MAXVALUE para dados que não se enquadrem nos valores estipulados nas partições existentes)