# Reconciliação de turnos com identificadores numéricos

Os turnos anteriores à correção ficam retidos pelo runtime quando qualquer evento
contém identificadores numéricos. Eles não são convertidos nem reenviados
automaticamente: o contato pode ter sido arredondado pelo Redis e o atendimento
pode já ter continuado. O alerta `pipeline.turn.claim_failed` indica essa condição.
Um evento novo para a mesma conversa também permanecerá retido até a reconciliação.

## Preparação antes da publicação

1. Validar em ambiente isolado a correção e o processamento de um evento novo com
   ID de contato acima de 2^53. Confirmar uma resposta e deduplicação do postback.
2. Planejar janela operacional, responsável, backup recuperável e proteção dos dados
   do Redis (os turnos podem conter mensagens e credenciais). Pausar a entrada e os
   workers antes de qualquer manutenção da fila; observar leases até expirarem.
3. Inventariar somente metadados: chave, revisão, fase, IDs das mensagens, recibos,
   tentativas e existência de resposta salva. Correlacionar a chave exata com a
   conversa e os IDs originais no CRM, sem confiar no número arredondado do JSON.
   A chave contém o ID íntegro usado na entrada, mas essa relação deve ser conferida
   no CRM antes de qualquer ação. Não registrar texto, tokens ou payload completo.
4. Para cada conversa, verificar mensagens e respostas posteriores, status,
   responsável, etiquetas, configuração do bot e efeitos externos já executados.
   Classificar como: atendimento já realizado/obsoleto; ainda exige atendimento;
   ou incerto. Hellen #4729 já teve resposta humana e deve ser tratada como
   obsoleta para resposta automática, sujeita à conferência atual.

## Execução posterior, mediante autorização de produção

A ferramenta `reconcile`, incluída na imagem corrigida, processa **uma chave por
vez** e nunca chama a IA ou envia mensagens. Somente um operador autorizado deve
executá-la após parar a entrada do CRM e todos os workers do runtime, confirmar
que não há chamadas externas em andamento e aguardar o término das leases. A flag
`--workers-stopped` registra a confirmação do operador; ela não detecta parada de
processos nem impede que sejam reiniciados. Um backup externo recuperável é
obrigatório antes de qualquer arquivamento.

Com `REDIS_URL` fornecida pelo ambiente seguro, primeiro inspecione:

```sh
./reconcile --action inspect --key 'bot_runtime:durable:CONTATO:CONVERSA'
```

O resultado fornece identificadores de mensagens, fase, revisão, presença de
resposta salva, lease e SHA-256 do payload, sem exibir conteúdo ou credenciais.
Cruze os IDs com o CRM e revise o histórico até o momento da decisão. Não use o
ID arredondado do payload para identificar o contato. Para `human_handoff`, o
operador deve antes encaminhar o atendimento pelo fluxo humano e documentá-lo;
a ferramenta não altera o CRM nem verifica que isso foi feito.

Para um turno legado sem resposta salva e após a decisão documentada, use o hash
exato da inspeção, identificador do operador e protocolo de revisão:

```sh
./reconcile --action archive --key 'bot_runtime:durable:CONTATO:CONVERSA' \
  --sha256 'HASH_DA_INSPECAO' --decision obsolete --operator 'OPERADOR' \
  --ticket 'PROTOCOLO' --workers-stopped
```

A decisão também pode ser `human_handoff`. A operação verifica atomicamente o
payload completo e a ausência de lease, arquiva o payload original em uma chave
protegida do mesmo Redis, grava auditoria, remove o turno da fila e conserva os
recibos. Não apaga histórico nem envia resposta. Um evento novo antes da operação
invalida o hash; recomece a revisão. Uma falha de rede pode ocultar o resultado:
consulte a chave de arquivo e auditoria antes de repetir. O arquivo contém dados
sensíveis e exige controle de acesso, backup e política de retenção. A cópia no
mesmo Redis não substitui o backup externo. Turnos com resposta salva, fase fora
 de `waiting`, dados inválidos ou dúvida operacional permanecem retidos para
análise especializada. Não editar manualmente o JSON nem apagar os recibos.

O arquivamento permite que futuras mensagens sejam recebidas normalmente; não
reprocessa as mensagens antigas. Reabra gradualmente a entrada após conferir os
resultados, monitorar fila e alertas e validar um atendimento novo. Não habilitar
replay em lote nem usar FLUSHDB. Uma reversão deve preservar o Redis e considerar
que o runtime antigo não entende IDs em texto.
