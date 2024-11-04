Classe principal: ~/src/handlers/lambda_handler.py

Teste integrado: ~/tests/integration/runner.py

Variaveis ambientes:
- application_name (nome do projeto, reflete no nome do grupo de logs do cloudwatch)
- enable_drop (ativa/desativa fluxo de drop partition)
- enable_create (ativa/desativa fluxo de criacao de partition)
- database_host (host do banco SQL)
- database_user (user do banco SQL)
- database_password (password do banco SQL)
- database_name (database do banco SQL)
- table_name (table do banco SQL)
- future_partition_months (numero de partições mensais futuras a serem criadas)
- months_to_keep (valor a ser usado para a quantidade de meses que deseja manter)
  - ex: Se hoje é 25/10/2024 e o valor utilizado for 2, irá excluir a partição do mes 7 pra trás, mantendo as do mes 10, 9 e 8
- maxvalue_partition_name (o nome da particao MAXVALUE para dados que não se enquadrem nos valores estipulados nas partições existentes)
