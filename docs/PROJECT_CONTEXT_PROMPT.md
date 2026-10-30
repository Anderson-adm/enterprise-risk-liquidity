# Prompt de contexto do Case 01 — atualizado em 30/10/2026 (após correção e refatoração offline)

Copie o bloco abaixo para continuar a discussão técnica em outro chat. As evidências AWS
registram eventos anteriores; nenhum recurso foi recriado para produzir este resumo.

```text
Estou discutindo meu projeto de portfólio Case 01 — Enterprise Risk & Liquidity.
Use este contexto como estado factual e diferencie implementação, validação local,
provisionamento AWS e execução funcional. Não presuma que o pipeline completo passou na AWS.

Repositório público:
https://github.com/Anderson-adm/enterprise-risk-liquidity

1. OBJETIVO E ARQUITETURA
Reconstrução autoral de integração de dados financeiros, com dados, nomes e regras
sintéticos. Não são regras regulatórias nem dados de empresas reais.
Sete fontes: duas bases PostgreSQL (fund_ops, portfolio_risk), duas bases SQL Server
(legacy_finance com views legadas e exposure_compliance), CSV de preços, XML interno
e XML externo de transferência. São 13 entidades Bronze.
Fluxo híbrido: fontes locais → extração/Landing S3 → Lambda/Glue → Bronze com
manifestos/checksums/quarentena → dbt/Athena e Iceberg Silver/Gold → gate de qualidade
→ Glue Gold Load gera pacote S3 → publisher local publica transacionalmente no SQL Server.
Airflow orquestra; CloudWatch/SNS/EventBridge observam; Terraform define infraestrutura.
Os produtos são liquidity_summary e exposure_compliance. Valores usam Decimal/DECIMAL.
Fontes, consumidor SQL Server e Airflow permanecem locais; AWS não acessa localhost.
Há três jobs Glue 5.0 em Python/PySpark: db-gb e db-tb compartilham db_ingest.py;
gold-load usa gold_load.py. GB/TB são padrões de ingestão, não benchmarks de volume.

2. O QUE FOI VALIDADO LOCALMENTE
Pipeline completo com DuckDB/dbt, PostgreSQL 17 e SQL Server 2022 reais em containers,
além de SQLite como alternativa. Sete fontes, oito cenários, idempotência, reprocessamento,
quarentena, bloqueio por qualidade e preservação da última publicação válida.
Publisher SQL Server validado com quatro publicadores concorrentes.
Airflow 3.3.2 validado localmente em sucesso e falha de qualidade.
Revisão independente: 72 testes rápidos + 10 dbt + 24 integrações = 106 casos locais;
13 testes Terraform com provider mockado; Ruff/TFLint passaram; Checkov 148 passed,
0 failed, 36 skipped, com exceções documentadas para laboratório sintético.
Correções já publicadas incluem reversão A→B→A, snapshots vazios, visibilidade somente
de Bronze com commit, remoção de chaves desaparecidas no Gold, precisão decimal,
gate sem artefato dbt obsoleto, locks de publicação e controles de conta/perfil/região.
As correções da revisão estão no commit 5fe19db; o baseline publicado antes desta
atualização documental é f1e9fa5, com CI verde. Esses testes não validam o runtime AWS.

3. O QUE REALMENTE OCORREU NA AWS
Foram usados SSO data-dev, conta esperada conferida por ID e região sa-east-1.
Bootstrap Terraform: 7 recursos; dev: 69 recursos. Ambos aplicados e verificados por
leituras reais: S3, Glue Catalog/jobs, Lambda, IAM, Athena workgroup, logs/alarmes,
SNS, EventBridge e orçamento mensal de US$ 5 para toda a conta.
Glue configurado com 2 workers G.1X, timeout de 15 minutos e retries zero.
Athena com engine 3, resultados criptografados e corte de 100 MiB por consulta.
O orçamento alerta, mas não limita gastos automaticamente.

Houve uma única tentativa funcional autorizada para 2026-10-26:
run 20261030T000957Z-93958f (o ID usa UTC).
A mesma entrada passou antes na referência local: 8 linhas por produto Gold,
gate aprovado e nenhuma rejeição. Usamos cinco bases locais novas aws1_*.
Na AWS, extração e Landing passaram, deixando 24 objetos. A primeira invocação,
erl-dev-ingest-external-xml, falhou ao escrever Parquet:
ArrowNotImplementedError: Support for codec 'zstd' not built.
O writer bronze.parquet_bytes usa compression="zstd". A layer AWS gerenciada
AWSSDKPandas-Python312:32 contém PyArrow 24.0.0 sem esse suporte; o PyArrow local
26.0.0 suporta Zstandard. Importação e tamanho compatíveis não provaram o codec.
A Lambda durou 8,623 s, com 9,390 s faturáveis e pico de 220/512 MiB.
Não houve retry. Ficou um XML bruto Bronze sem manifesto de commit.
Nenhum Glue job, dbt/Athena, Silver/Gold Iceberg, snapshot ou publicação SQL Server
foi executado/produzido na AWS. Portanto não existe comparação financeira AWS/local
concluída. CloudWatch registrou o erro real e uma ação SNS bem-sucedida; a assinatura
foi confirmada, mas o recebimento humano do alerta não foi verificado.

4. CUSTOS E ENCERRAMENTO
A previsão de planejamento era US$ 1–3 para uma data, sem ser teto garantido.
Preços regionais consultados: Glue US$ 0,69/DPU-hora; Athena US$ 9/TB.
Na tentativa interrompida: estimativa Lambda ≈US$ 0,00007845 e PUTs mínimos
≈US$ 0,000175. O subtotal ≈US$ 0,000253 é parcial: não mede toda requisição,
armazenamento, logs, SNS ou alarmes; não é uma fatura. Não foi usado Cost Explorer.

O proprietário decidiu parar os gastos e autorizou exclusão integral.
Os 69 recursos dev e 7 de bootstrap foram destruídos, incluindo os três buckets,
seus objetos/versões e o bucket de state. A ausência dos 76 foi verificada.
Depois ampliou a autorização aos recursos pré-existentes da conta data-dev:
mais quatro buckets e suas 27 versões/marcadores, uma Lambda, log group e role
de aplicação foram removidos. O inventário cobriu 17 regiões habilitadas e serviços
globais e não encontrou outros recursos cobrantes nos serviços consultados,
acordos Marketplace ativos, Savings Plans ou reservas ativas verificadas.
Permaneceram identidades de acesso/serviço, defaults gratuitos e alias/aws/lambda,
chave AWS gerenciada não excluível pelo cliente e sem tarifa de armazenamento.
Regiões desabilitadas não foram habilitadas. Fatura/plano de Suporte não foram
independentemente auditados: não afirmar conta literalmente vazia ou fatura zero.
States, configurações e evidências brutas foram preservados privadamente/localmente;
somente evidências sanitizadas são públicas. Data-prod/default não foram usados.

5. CORREÇÃO E REFATORAÇÃO OFFLINE (30/10/2026, commits locais, sem AWS)
Codec: o contrato Bronze declara o codec Parquet (zstd); os writers provam o codec com um
round-trip antes da primeira escrita (sem objetos órfãos); o ZIP da Lambda passou a embutir
PyArrow/lxml fixados no uv.lock, sem a layer gerenciada (~54 MiB, acima do limite de upload
direto, por isso via S3). A layer exata não pôde ser reproduzida offline; o ZIP final foi
testado localmente em interpretador isolado com S3 simulado (moto). Não houve novo deploy.
Gerador: papéis de cenário posicionais (D1 limpa, D2 problemas, D3 chegada tardia), datas
estritamente crescentes, geração estável por prefixo; uma data gera só a D1 limpa.
Estrutura: src/python/{connect,extract,transform,load,ingestion,storage,utils}, pipeline e
sources; src/spark/{readers,transforms,writers}; SQL versionado em src/sql; contratos em
src/contracts; entrypoints finos lambda/bronze, glue/bronze, glue/load; build pré-Terraform em
scripts/build -> dist/ com manifest (SHA-256, commit, runtime) e smoke dos artefatos; bucket de
artefatos separado do state e do data lake. O wrapper src/erl foi removido.
Validação local após a refatoração: 149 casos (106 históricos + 43 novos), Terraform com
14 testes mockados, TFLint 0, Checkov 162/0/40; comparação exata antes/depois dos oito
cenários em SQLite e em PostgreSQL+SQL Server. Spark não foi executado (sem JVM local) e
nenhum Glue/Athena/Iceberg rodou na AWS.

6. PENDÊNCIAS E DIREÇÃO DISCUTIDA
Ainda faltam execução real de Glue, instalação das dependências, Ion SerDe,
dbt/Athena/Iceberg, comparação decimal e publicação SQL Server em modo híbrido.
Código modular e artefatos com hash no S3 (chaves estáveis + bucket versionado) foram
implementados offline em 30/10; releases imutáveis por hash e CI/CD com OIDC continuam
apenas discutidos. A CI testa o repositório e constrói/testa os artefatos; não faz deploy AWS.
Hoje não há laboratório AWS do Case 01 ativo. Não há autorização vigente para
recriar recursos, fazer deploy, gastar ou repetir o pipeline. Qualquer retomada
exige decisão explícita e novos planos; os antigos foram consumidos e o backend removido.

Evidências principais no repositório:
- docs/HANDOFF.md e docs/INDEPENDENT_REVIEW.md
- evidence/aws/dev-apply/verification.json
- evidence/aws/functional/2026-10-26/summary.json e lambda_cloudwatch.log
- evidence/aws/teardown/verification.json
- evidence/aws/account-cleanup/verification.json

Ajude-me a discutir maturidade, arquitetura, limites e próximos passos com base nesses
fatos. Priorize melhorias justificadas e trabalho offline; não apresente sugestões
como já implementadas, não prometa custo zero e não confunda infraestrutura
provisionada com pipeline validado na AWS.
```
