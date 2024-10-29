@all
Feature: Gerenciamento de Partições

  Scenario: Criar 1 partição futura com sucesso e drop 1 partição passada
    When conto a quantidade de partições existentes
    And faço drop de uma partição passada usando o valor "1" como indexador passado
    And valido que a partição não existe mais
    And valido que a quantidade de partições é menor agora
    Then valido que a partição maxvalue "catch_all" não contém itens
    And crio "1" partição futura
    And valido que a particao existe
    And que o numero de partições não tenha alterado

  Scenario: Criar 5 partição futuraS com sucesso e drop 1 partição passada
    When conto a quantidade de partições existentes
    And faço drop de uma partição passada usando o valor "1" como indexador passado
    And valido que a partição não existe mais
    And valido que a quantidade de partições é menor agora
    Then valido que a partição maxvalue "catch_all" não contém itens
    And crio "5" partição futura
    And valido que a particao existe
    And valido que a quantidade de partições é maior agora

  Scenario: Criar 5 partições futuras com sucesso
    When valido que a partição maxvalue "catch_all" não contém itens
    And conto a quantidade de partições existentes
    Then crio "5" partição futura
    And valido que as partições existam
    And valido que a quantidade de partições é maior agora

  Scenario: Criar 1 partição futura e inserir dados, garantindo que todos eles permaneçam no range aceitável da partição
    When valido que a partição maxvalue "catch_all" não contém itens
    And conto a quantidade de partições existentes
    Then crio "1" partição futura
    And valido que a particao existe
    And valido que a quantidade de partições é maior agora
    When insiro itens com datas "5" anos no futuro
    And valido que a(s) partição atual contém itens
    And verifico se o campo min e max da chave de particionamento da particao criada está dentro do range aceitavel
    And valido que a partição maxvalue "catch_all" contém itens

  Scenario: Criar 5 partições futuras e inserir dados, garantindo que todos eles permaneçam no range aceitável das partições
    When valido que a partição maxvalue "catch_all" não contém itens
    And conto a quantidade de partições existentes
    Then crio "5" partição futura
    And valido que as partições existam
    And valido que a quantidade de partições é maior agora
    Then insiro itens com datas "5" meses no futuro
    And valido que a(s) partição atual contém itens
    And verifico se o campo min e max da chave de particionamento da particao criada está dentro do range aceitavel
    And valido que a partição maxvalue "catch_all" não contém itens

  Scenario: Drop de 1 partição com sucesso
    When conto a quantidade de partições existentes
    Then faço drop de uma partição passada usando o valor "1" como indexador passado
    And valido que a partição não existe mais
    And valido que a quantidade de partições é menor agora

  Scenario: Itens inseridos que não se enquadrem no range de nenhuma partição devem ir para a partição Maxvalue
    When valido que a partição maxvalue "catch_all" não contém itens
    Then crio "1" partição futura
    And insiro itens com datas "6" anos no futuro
    And valido que a partição maxvalue "catch_all" contém itens

  Scenario: Retornar erro quando a partição Maxvalue conter itens e for tentar criar uma nova partição
    When conto a quantidade de partições existentes
    And insiro itens com datas "10" anos no futuro
    And valido que a partição maxvalue "catch_all" contém itens
    Then crio "1" partição futura e espero que ocorra um erro
    And que o numero de partições não tenha alterado