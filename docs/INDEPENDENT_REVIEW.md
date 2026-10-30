# Case 01 — Revisão independente de engenharia

> **Historical document.** File links and paths below refer to the layout before the
> 2026-10-30 refactoring (`src/erl/...`, `glue/jobs/...`); see
> [REFACTORING.md](REFACTORING.md) for where each module lives now. Content is unchanged.

Codex · 2026-10-29 · base da revisão `13f9820` · correções em `5fe19db`.
Publicação autorizada pelo proprietário no fechamento; estado do CI em [HANDOFF.md](HANDOFF.md).

**Conclusão: SIM COM RESSALVAS para iniciar o Marco 4 — validação AWS controlada.**
Os defeitos abaixo foram corrigidos e os checks locais foram reproduzidos. Athena, Iceberg,
Glue/Spark, Lambda e alertas em AWS continuam **implementados, não validados em AWS**.
Nenhuma API AWS, consulta paga, criação de recurso, `apply` ou `destroy` foi executado nesta revisão.

Foram examinados AGENTS.md, CLAUDE.md, HANDOFF, CODEX_REVIEW, DATA_MODEL, TERRAFORM,
AWS_VALIDATION, README, contratos, implementação Python/SQL/dbt, Glue, Airflow, Terraform,
empacotamento, teardown, testes e CI. A arquitetura das sete fontes foi mantida; o repositório
do portfólio não foi alterado.

**Achados e correções.** P0 indica risco de resultado incorreto, exposição de dados parciais
ou quebra do bloqueio de publicação; P1 indica confiabilidade, compatibilidade ou proteção
operacional; P2 indica ferramenta de verificação enganosa. São defeitos reproduzidos ou
incompatibilidades estáticas identificadas, sem alegação de incidentes em dados reais.

| ID / prioridade | Local afetado | Consequência anterior | Correção e verificação |
|---|---|---|---|
| R01 · P0 | [bronze.py](../src/erl/ingestion/bronze.py), `find_accepted_duplicate`; [files.py](../src/erl/ingestion/files.py); [db_ingest.py](../glue/jobs/db_ingest.py) | A → B → A descartava a última A como duplicata histórica, deixando B vigente. Cadastros também comparavam a data errada; XML precisava manter escopo por carteira. | Comparar somente a entrega aceita vigente, por snapshot ou cadastro global. Manifestos de arquivos incluem `snapshot_key`. Regressões de arquivo e cadastro falharam antes; passam depois. Helper Glue exercitado sem Spark. |
| R02 · P0 | [contrato Bronze v2](../src/erl/contracts/bronze_contract.json), [database.py](../src/erl/ingestion/database.py), [platform.sql](../dbt/macros/platform.sql) | Snapshot vazio não carregava metadados de versão em nenhuma linha; os oito saldos anteriores permaneciam no Silver. | Marcador apenas de metadados, `meta_row_number=0`; ordenar entregas antes de excluir o marcador. Contagens continuam representando linhas de negócio. Teste observou 8 linhas antes e 0 após a correção. |
| R03 · P0 | `latest_delivery` em [platform.sql](../dbt/macros/platform.sql); [catalog.tf](../infra/environments/dev/catalog.tf) | Uma falha entre escrever Bronze e escrever o manifesto deixava dados sem commit visíveis ao Silver. | Silver exige manifesto ACCEPTED correspondente a fonte, entidade, run e objeto. Tabela de controle declarada em Terraform com Ion SerDe. Falha injetada reproduz a exposição anterior; após a correção, a versão aceita permanece visível. Leitura Ion/Athena aguarda AWS. |
| R04 · P0 | [dois modelos Gold](../dbt/models/gold/), `gold_snapshot` em [platform.sql](../dbt/macros/platform.sql) | Remover uma carteira da origem a retirava do consumidor, mas deixava a linha antiga no Gold, inclusive no DuckDB. Filtrar o pacote pelo run não corrigia o lakehouse. | Um MERGE por produto atualiza/insere chaves e exclui as desaparecidas da mesma data. Marcadores de exclusão nunca entram no destino. Cenário end-to-end falhou antes e passa depois: 7 carteiras no Gold e na publicação. Outras datas são preservadas. |
| R05 · P0 | `step_transform` / `evaluate_gate` em [pipeline.py](../src/erl/pipeline.py) | Código dbt não zero podia passar pelo gate; falha de parse em retry podia reutilizar `run_results.json` anterior. | Descartar resultado antigo antes de invocar dbt; exigir resultado novo e considerar o código de saída no gate. Duas regressões falharam antes e passam depois. |
| R06 · P0 | `step_ingest` em [pipeline.py](../src/erl/pipeline.py); `validate_product` em [publisher.py](../src/erl/publish/publisher.py) | CSV obrigatório inteiramente rejeitado podia recorrer a preços antigos; pacote com produtos vazios podia apagar a última publicação. | Ingestão exige entrega aceita/duplicada da fonte obrigatória nesta execução. Produtos vazios são recusados antes de publicar. Regressões reproduzidas antes/depois. |
| R07 · P0 | Macro `ratio` em [platform.sql](../dbt/macros/platform.sql); [candidatos Gold](../dbt/models/gold/) | DuckDB dividia decimais por DOUBLE: `999999999999.99 / 3.00` virava `333333333333.330048` no DECIMAL final. No Trino, dividir dinheiro de escala 2 preserva apenas essa escala antes do cast final. | DuckDB usa divisão inteira exata, arredondamento half-up e resultado decimal de 12 casas; Athena aumenta a escala dos operandos antes da divisão. O exemplo local agora retorna `333333333333.330000`. A parte Athena é correção baseada em regras documentadas, ainda sem execução no serviço. |
| R08 · P1 | Identificador da tentativa em [publisher.py](../src/erl/publish/publisher.py) | Contagem fora do lock gerava o mesmo ID para quatro retries do mesmo pacote; SQL Server rejeitava três com chave duplicada e também falhava ao registrar esses erros. | ID único por tentativa, mantendo o lock transacional existente. Regressão real: uma ACTIVE, três SKIPPED_IDENTICAL e quatro logs distintos. As falhas de rollback e supersessão existentes continuam passando. |
| R09 · P1 | Checkpoints em [pipeline.py](../src/erl/pipeline.py), [extract.py](../src/erl/sources/extract.py), [bronze.py](../src/erl/ingestion/bronze.py), [db_ingest.py](../glue/jobs/db_ingest.py) | Repetir etapas ou entregas concluídas com o mesmo run tentava sobrescrever objetos imutáveis. | Reutilizar estado SUCCEEDED, manifestos de extração/ingestão concluídos e pacote comprometido; coleta reconhece bytes já escritos. Run não pode trocar de data. Regressões de etapa, arquivo e extração falharam antes. Interrupção antes do primeiro commit ainda exige novo run, conforme limites abaixo. |
| R10 · P1 | `step_report` em [pipeline.py](../src/erl/pipeline.py) | Warehouse incompleto fazia o relatório falhar por Silver inexistente e escondia a falha original de fonte/transformação. | Erro de diagnóstico do DuckDB é registrado no relatório; causa original e classificação permanecem. Regressão com warehouse vazio falhou antes e passa depois. |
| R11 · P1 | [glue/requirements.txt](../glue/requirements.txt), [glue.tf](../infra/environments/dev/glue.tf) | Glue 5.0 embarca boto3 1.34.131, anterior ao `PutObject IfNoneMatch` utilizado pelos scripts. O primeiro deploy não declarava o SDK necessário. | SDK e dependências transitivas fixados conforme `uv.lock`, instalados por `--additional-python-modules`. Consistência verificada localmente; instalação/execução em Glue não foi realizada. |
| R12 · P1 | `put_part_if_absent`, listagem de partes em [db_ingest.py](../glue/jobs/db_ingest.py) | HEAD seguido de COPY não impedia sobrescrita concorrente do destino e requeria leitura Bronze ausente no IAM. Listagem sem paginação podia ignorar partes além da primeira página. | PUT condicional por parte, sem HEAD/COPY; listagem e limpeza paginadas. Teste S3/moto prova recusa de sobrescrita. O driver lê uma parte por vez; não há validação de desempenho em TB. |
| R13 · P1 | [observability.tf](../infra/environments/dev/observability.tf), [glue.tf](../infra/environments/dev/glue.tf), [iam.tf](../infra/environments/dev/iam.tf) | `alias/aws/sns` não permite os publishers de serviço necessários; configuração de logs era da geração anterior ao Glue 5.0; faltava `PutMetricData`. | Exceção explícita de criptografia SNS para alertas sintéticos; política de serviço no tópico exato; prefixo de logs 5.0 e grupos error/output com retenção; métricas limitadas ao namespace Glue. Checks estáticos passam; entrega real de alertas segue pendente. |
| R14 · P1 | [runtime.py](../src/erl/aws/runtime.py), [config.py](../src/erl/config.py), [CLI](../src/erl/cli.py), providers/variáveis/backend Terraform | Sessão implícita podia usar `default`; invocar etapa isolada podia pular a checagem de conta. Região era configurável fora da permitida. | Perfil não default explícito, credenciais SSO/assume-role, região sa-east-1 e checagem de conta nas etapas AWS. Providers e exemplo de backend explícitos. Regressão de perfil falhou antes; mocks e testes Terraform verificam as recusas. |
| R15 · P1 | [teardown_aws.py](../scripts/teardown_aws.py) | Tags comuns também podiam aceitar o state bucket; nomes dos bancos não eram conferidos; erros parciais de exclusão podiam ser contados como sucesso. | Exigir tags do laboratório dev, nomes exatos Silver/Gold e propagação de erros de S3/Glue; perfil/região guardados. Regressões simuladas de bucket state e AccessDenied falharam antes. Nenhuma exclusão AWS foi executada. |
| R16 · P1 | `S3Store.exists` em [storage.py](../src/erl/storage.py) | HEAD de chave ausente pode retornar 403 quando a permissão de listagem é limitada por prefixo, impedindo verificações de checkpoint. | Listagem com o prefixo exato e comparação da chave, mantendo o escopo IAM existente. Mock com HEAD negado e chave de prefixo semelhante falha antes e passa depois. |
| R17 · P2 | [terraform_checks.sh](../scripts/terraform_checks.sh) | `&&`/`|| true` mascaravam falhas de fmt, TFLint e Checkov. Era possível terminar com exit 0 e anunciar checks aprovados incorretamente. | Propagar todos os códigos de saída com `set -euo pipefail`; três testes injetam falhas dos executáveis em diretórios temporários. A CI já invocava os checks diretamente. |

Os pontos iniciais podem ser localizados pelas funções acima; os testes novos estão em
[test_review_regressions.py](../tests/test_review_regressions.py),
[test_aws_components.py](../tests/test_aws_components.py),
[test_pipeline_e2e.py](../tests/test_pipeline_e2e.py) e
[test_integration_containers.py](../tests/test_integration_containers.py).
O total aumentou de 79 para 106 casos coletados: 27 novos casos, incluindo parametrizações.

**Resultados reproduzidos.** A base original passou em 47 testes rápidos + 9 dbt = 56 locais,
e em 23 testes de integração. Os primeiros acessos a TCP/sockets locais foram bloqueados pelo
sandbox; repetidos com a permissão local necessária, os testes passaram. As falhas iniciais
estão registradas e não foram tratadas como defeitos do produto.

| Verificação | Resultado da revisão | Evidência |
|---|---|---|
| Ruff lint e format | passaram | [verification.json](../evidence/review/verification.json) |
| Testes rápidos, contratos, mocks e regressões | 72 passaram | [final_unit.txt](../evidence/review/final_unit.txt) |
| dbt/DuckDB end-to-end | 10 passaram | [final_dbt.txt](../evidence/review/final_dbt.txt) |
| PostgreSQL + SQL Server reais, bases `it_*` | 24 passaram | [final_integration.txt](../evidence/review/final_integration.txt) |
| Airflow 3.3.2 com bancos reais, bases `review_*` | sucesso; falha de qualidade com DAG failed, gate failed, load/publish upstream_failed e report success | [airflow_summary.json](../evidence/review/airflow_summary.json) |
| Consumidor após falha Airflow | uma publicação ACTIVE preservada, 8 linhas de cada produto | [airflow_consumer_after_failure.json](../evidence/review/airflow_consumer_after_failure.json) |
| Terraform fmt/validate e testes com AWS mock | 3 diretórios válidos; 13 testes passaram | [final_terraform.txt](../evidence/review/final_terraform.txt) |
| TFLint 0.65.0 / AWS ruleset 0.50.0 | 0 problemas nos 3 diretórios | [final_tflint.txt](../evidence/review/final_tflint.txt) |
| Checkov 3.4.0 | 148 passaram, 0 falharam, 36 skips | [final_checkov.txt](../evidence/review/final_checkov.txt) |
| dbt-athena 1.11.1 / dbt-core 1.12.6 | parse offline passou; nenhuma query executada | [athena_parse.txt](../evidence/review/athena_parse.txt) |
| Pacote Lambda | build local passou, lxml 6.1.3 | [lambda_build.txt](../evidence/review/lambda_build.txt) |
| CI público anterior | três jobs success em `22c6c5f`; não cobre este diff local | [run](../evidence/review/github_ci_run.json), [jobs](../evidence/review/github_ci_jobs.json) |

As falhas antes das correções estão nos arquivos `*_before.txt` e
`regressions_before.txt` de [evidence/review/](../evidence/review/). São falhas esperadas de
regressão, distintas dos resultados finais. `verification.json` registra comandos, saídas e
hashes finais. A revisão usou ferramentas já instaladas e baixou apenas o wheel fixado de
lxml para o pacote local; nenhum pacote de sistema ou configuração global foi alterado.

As 33 exceções Checkov originais foram reavaliadas por controle e recurso. Foram mantidas
apenas no escopo de laboratório temporário/sintético: SSE-S3, região única, ausência de logs
de acesso S3, invocação Lambda síncrona sem DLQ, ausência de VPC, CMK e assinatura, retenção
curta e bookmarks desativados. Os riscos por controle estão em
[TERRAFORM.md](TERRAFORM.md). O total agora é 36: +1 SNS e +2 instâncias dos controles de
criptografia/retenção no segundo grupo Glue. A exceção SNS não foi escondida atrás de uma
configuração de criptografia incompatível.

**Pendências e limites operacionais.**

* AWS permanece sem evidência real. Validar instalação do SDK em Glue 5.0, dependências/layer
  Lambda, leitura Ion dos manifestos, IAM efetivo, CTAS/MERGE Iceberg com exclusões,
  precisão/igualdade dos produtos, logs, SNS e teardown em execução autorizada. Parse e mocks
  não demonstram esses comportamentos no serviço.
* A execução suportada é serial. Airflow limita seu DAG a um run ativo, mas não existe lock
  global entre CLI e Airflow. Não executar dois pipelines ao mesmo tempo. A concorrência de
  publicadores, separadamente, está testada em SQL Server e PostgreSQL.
* Manter fontes sintéticas estáveis durante a extração: os SELECTs das entidades usam
  conexões separadas, sem snapshot transacional único entre tabelas/sistemas.
* Checkpoints concluídos são reutilizáveis. Interrupção antes de concluir manifesto/estado
  pode deixar objetos imutáveis órfãos: usar novo run id. Bronze sem commit fica invisível ao
  Silver e pacote sem manifesto não é publicado. Recuperação de falhas arbitrárias e escala
  GB/TB não foram comprovadas.
* Cada tabela Gold tem seu próprio commit. Uma falha de engine entre os dois commits pode
  deixar produtos Gold de runs diferentes; o gate impede o Load/publicação desse run. O
  consumidor publica ambos em uma transação e preserva a última versão válida.
* Limites DECIMAL do consumidor continuam sendo limites de contrato. A divisão local exata
  cobre entradas financeiras não negativas com as escalas dos modelos; totais fora do
  contrato devem falhar. Paridade numérica de Athena precisa ser verificada no Marco 4.
* S3 sem auditoria de objetos, alertas sem KMS específico, retenção curta e SQL Server sem
  RCSI são restrições explícitas do laboratório. Consultas do consumidor podem bloquear
  durante a publicação, preservando consistência.
  Preços regionais não foram validados: alarmes e logs retidos também podem gerar custo em
  repouso. Configurar `alert_email` e confirmar a assinatura; o default não cria destinatários
  de e-mail nem notificações do budget. O budget não limita gastos.
* Na parada final dos containers, PostgreSQL encerrou com código 0 e SQL Server com 137,
  inclusive usando timeout de 60 s; `OOMKilled=false`. Os testes passaram antes da parada.
  Os volumes foram preservados. O comportamento de encerramento do SQL Server permanece
  registrado, sem alteração de Docker/WSL: [diagnóstico](../evidence/review/container_shutdown_diagnostics.txt).

**Prontidão para o Marco 4.** Os impedimentos encontrados para a primeira execução foram
corrigidos localmente e os checks passaram. É tecnicamente razoável iniciar uma validação
AWS pequena e serial, seguindo [AWS_VALIDATION.md](AWS_VALIDATION.md), com conta data-dev,
SSO, sa-east-1, plano salvo revisado, autorização humana e inventário/teardown controlados.
A revisão não autoriza essas operações e não transforma implementação estática em validação AWS.

Referências usadas nas correções estáticas: [aritmética decimal Trino](https://trino.io/docs/current/functions/decimal.html),
[MERGE transacional Athena v3](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-merge-into.html),
[configuração dbt-athena](https://docs.getdbt.com/reference/resource-configs/athena-configs),
[bibliotecas Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html),
[logs Glue 5.0](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuous-logging.html),
[SNS/KMS](https://docs.aws.amazon.com/sns/latest/dg/sns-key-management.html),
[política EventBridge/SNS](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-use-resource-based.html)
e [SerDes JSON/Ion](https://docs.aws.amazon.com/athena/latest/ug/json-serde.html).
O comportamento de chave ausente foi conferido na referência de
[S3 HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html).
As categorias de cobrança de monitoramento estão em
[CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/); não foi feito orçamento regional validado.
