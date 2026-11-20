# Fila durável v2: implantação e reconciliação

A versão v2 armazena cada evento como texto JSON, preservando IDs inteiros durante as operações Lua. Ela usa chaves e fila próprias. A fila v1 e seus dados não são migrados nem reproduzidos automaticamente. Erros de decodificação na v2 movem o turno para a quarentena sem apagar o payload. Eventos novos para uma conversa com turno v1 pendente, recibo v1 correspondente ou turno v2 em quarentena são rejeitados para posterior reconciliação.

O CRM deve receber antes a alteração que reconhece `409 ERR_TURN_BLOCKED` sem registrar falha no circuito. Somente após verificar essa versão, iniciar o runtime v2. O retorno 409 mantém a entrega no outbox, sujeito às regras de elegibilidade e repetição do CRM.

## Antes de publicar

1. Planejar uma interrupção controlada, garantindo que o emissor retenha eventos não confirmados. Fazer backup seguro das chaves e inventariar turnos, recibos e quarentenas sem expor dados de clientes.
2. Parar e drenar completamente todos os processos antigos antes de iniciar a v2. Versões v1 e v2 simultâneas podem processar a mesma conversa e enviar respostas duplicadas.
3. Validar em ambiente isolado, acompanhar erros de aceitação, quarentena, fila e tempo de processamento. Confirmar que eventos rejeitados pelo runtime continuam no outbox do CRM para nova tentativa. A indisponibilidade ou o esgotamento de tentativas do outbox exige tratamento operacional.
4. Para cada turno v1, comparar mensagens e situação atual no CRM, intervenções humanas, respostas já enviadas e validade do conteúdo. Decidir individualmente se há atendimento pendente. Não executar automaticamente mensagens antigas nem apagar registros antes de reconciliação comprovada. A ferramenta e o procedimento de recuperação controlada ainda precisam ser implementados e validados.
5. Liberar novas mensagens de cada conversa somente após resolver seu turno legado ou sua quarentena de forma auditável. Não editar ou remover chaves apenas para contornar o bloqueio.

## Retorno à versão anterior

Parar os workers e preservar um snapshot antes de qualquer rollback. A versão antiga não processa a fila v2 e pode retomar turnos v1 antigos. Reconciliar ambas as filas e os efeitos já produzidos antes de religar qualquer worker; não basta voltar a imagem.

## Limitação

A v2 preserva os eventos na ordem de chegada ao runtime. Ela não garante ordenação cronológica entre módulos se eventos chegarem fora de ordem; isso depende de um contrato de tempo e ordenação verificável entre CRM e runtime. O payload continua contendo os dados operacionais do evento, portanto o acesso ao Redis e aos backups deve ser restrito.

## Inventário local somente leitura

O comando `go run ./cmd/durable-inventory` usa `REDIS_URL` do ambiente e lê as chaves v1, seus recibos e a fila legada. Ele não cria, remove, altera, reserva ou executa turnos. O relatório JSON lista identificadores, fase, quantidade de eventos, IDs de mensagens e quantidade de recibos; não inclui conteúdo, URLs ou credenciais. Identificadores são dados operacionais e o relatório deve ficar em local de acesso restrito.

O inventário não é um snapshot atômico. Para uma decisão operacional, interromper os workers conforme o plano de corte, registrar um backup seguro, executar novamente e comparar os IDs com as mensagens, entregas, respostas, status e intervenções humanas no CRM. Chaves com recibos sem turno também exigem análise: sua existência não comprova, por si só, que houve atendimento ao cliente. JSON inválido ou evento sem ID é sinalizado para revisão manual, sem tentar deduzir identidade ou restaurar números perdidos. Esse inventário não implementa liberação nem recuperação.

## Cruzamento com CRM

Um protótipo local no repositório do CRM, `rails runner script/bot_runtime_reconciliation_report.rb <inventario.json>`, compara o inventário com o banco. Ele não integra esta publicação e precisa de revisão própria antes de ser usado em produção. A conexão deve usar um usuário PostgreSQL com privilégios somente de leitura; o script também abre a transação como `READ ONLY`. Ele verifica a identidade do contato pela chave derivada, confere se as mensagens de origem pertencem à conversa, e informa status, horários e estados das entregas, sem conteúdo das mensagens. O JSON de saída contém identificadores e deve ser protegido como dado operacional.

Recibos ou horários não comprovam que uma resposta foi entregue ao cliente, e uma mensagem registrada não garante que ainda precisa de resposta. Toda linha exige revisão; fontes ausentes, chaves inconsistentes e registros sem IDs ficam em revisão manual. Não há mecanismo de liberação ou replay neste relatório. Validar a integridade e a origem do inventário e usar uma cópia consistente; a leitura do Redis e a leitura do PostgreSQL não constituem um snapshot conjunto.

## Handoff protegido

O runtime v2 envia os IDs das mensagens e do agente que originaram uma resposta de transferência. O CRM rejeita essa resposta quando a conversa já está concluída, quando falta alguma origem, quando o agente mudou ou quando uma mensagem pública posterior de cliente ou humano torna a resposta potencialmente desatualizada. O runtime encerra esse turno após receber a rejeição específica, sem insistir no envio. As versões legadas não enviam os IDs e mantêm o comportamento anterior durante a transição; aplicar o CRM atualizado antes do runtime v2. Mensagens criadas simultaneamente à validação não compartilham necessariamente o mesmo lock da conversa, portanto ainda é necessário validar essa janela em ambiente integrado antes de afirmar garantia completa de ordenação.
