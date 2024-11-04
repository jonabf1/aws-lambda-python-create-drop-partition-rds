@all
Feature: Gerenciamento de Partições

  Scenario: Criar 5 partição futuras com sucesso e manter apenas as ultimas 4 partições
    When conto a quantidade de itens existentes na tabela
    And faço drop de partições passadas usando o valor "4" como valor de referencia
    And valido que as partições não existem mais
    And valido que a quantidade de partições é menor agora
    Then valido que a partição maxvalue "catch_all" não contém itens
    And crio "5" partição futura
    And valido que as partições existem
    And verifica se o numero de particoes é igual a "11" contando com a MaxValue e a do mes atual
    And verificar se a quantidade de itens na tabela está de acordo com o esperado

  Scenario: Criar 1 partição futura com sucesso e manter apenas a ultima partição
    When conto a quantidade de itens existentes na tabela
    And faço drop de partições passadas usando o valor "1" como valor de referencia
    And valido que as partições não existem mais
    And valido que a quantidade de partições é menor agora
    Then valido que a partição maxvalue "catch_all" não contém itens
    And crio "1" partição futura
    And valido que a particao existe
    And verifica se o numero de particoes é igual a "4" contando com a MaxValue e a do mes atual
    And verificar se a quantidade de itens na tabela está de acordo com o esperado

  Scenario: Criar 5 partições futuras com sucesso
    When valido que a partição maxvalue "catch_all" não contém itens
    And conto a quantidade de itens existentes na tabela
    And conto a quantidade de partições existentes
    Then crio "5" partição futura
    And valido que as partições existem
    And valido que a quantidade de partições é maior agora
    And verificar se a quantidade de itens na tabela está de acordo com o esperado

  Scenario: Criar 1 partição futura e inserir dados, garantindo que todos eles permaneçam no range aceitável da partição
    When valido que a partição maxvalue "catch_all" não contém itens
    And conto a quantidade de itens existentes na tabela
    And conto a quantidade de partições existentes
    Then crio "1" partição futura
    And valido que a particao existe
    And valido que a quantidade de partições é maior agora
    When insiro itens com datas "5" anos no futuro
    And valido que a(s) partição atual contém itens
    And verifico se o campo min e max da chave de particionamento da particao criada está dentro do range aceitavel
    And valido que a partição maxvalue "catch_all" contém itens
    And verificar se a quantidade de itens na tabela está de acordo com o esperado

  Scenario: Criar 5 partições futuras e inserir dados, garantindo que todos eles permaneçam no range aceitável das partições
    When valido que a partição maxvalue "catch_all" não contém itens
    And conto a quantidade de itens existentes na tabela
    And conto a quantidade de partições existentes
    Then crio "5" partição futura
    And valido que as partições existem
    And valido que a quantidade de partições é maior agora
    Then insiro itens com datas "5" meses no futuro
    And valido que a(s) partição atual contém itens
    And verifico se o campo min e max da chave de particionamento da particao criada está dentro do range aceitavel
    And valido que a partição maxvalue "catch_all" não contém itens
    And verificar se a quantidade de itens na tabela está de acordo com o esperado

  Scenario: Drop de 1 partição com sucesso
    When conto a quantidade de partições existentes
    And conto a quantidade de itens existentes na tabela
    And faço drop de partições passadas usando o valor "1" como valor de referencia
    And valido que as partições não existem mais
    And valido que a quantidade de partições é menor agora
    And verificar se a quantidade de itens na tabela está de acordo com o esperado

  Scenario: Criação e drop de particao com as Flags desativadas
    When conto a quantidade de partições existentes
    And conto a quantidade de itens existentes na tabela
    And faço drop de partições passadas usando o valor "3" como valor de referencia e com as flags desativadas
    Then que o numero de partições não tenha alterado
    And verificar se a quantidade de itens permanece a mesma
    Then crio "1" partição futura com as flags desativadas
    And que o numero de partições não tenha alterado
    And verificar se a quantidade de itens permanece a mesma

  Scenario: Drop de partição sem conter partições (alem da atual e MaxValue)
    Given removo todas as partições exceto pela atual e MaxValue
    And conto a quantidade de partições existentes
    And conto a quantidade de itens existentes na tabela
    And faço drop de partições passadas usando o valor "6" como valor de referencia
    And verificar se a quantidade de itens na tabela está de acordo com o esperado

  Scenario: Itens inseridos que não se enquadrem no range de nenhuma partição devem ir para a partição Maxvalue
    When valido que a partição maxvalue "catch_all" não contém itens
    And conto a quantidade de itens existentes na tabela
    Then crio "1" partição futura
    And insiro itens com datas "6" anos no futuro
    And valido que a partição maxvalue "catch_all" contém itens
    And verificar se a quantidade de itens na tabela está de acordo com o esperado

  Scenario: Retornar erro quando a partição Maxvalue conter itens e for tentar criar uma nova partição
    When conto a quantidade de partições existentes
    And conto a quantidade de itens existentes na tabela
    And insiro itens com datas "10" anos no futuro
    And valido que a partição maxvalue "catch_all" contém itens
    Then crio "1" partição futura e espero que ocorra um erro
    And que o numero de partições não tenha alterado
    And verificar se a quantidade de itens na tabela está de acordo com o esperado