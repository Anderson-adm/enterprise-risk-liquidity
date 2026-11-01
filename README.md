# Enterprise Risk & Liquidity Data Platform

**Case 01 do meu portfólio de Engenharia de Dados** · [ver o case no portfólio](https://anderson-adm.github.io/data-engineer-portfolio-site/cases/risk-liquidity.html)

Reconstrução autoral de um projeto real do mercado financeiro: sete fontes heterogêneas
integradas em um lakehouse na AWS, com qualidade de dados como portão, reprocessamento
idempotente e publicação transacional para um sistema de controle e relatórios.

> Todos os dados, códigos, limites e regras deste repositório são **sintéticos**. Nada vem de
> nenhuma empresa real.

![Arquitetura da plataforma](docs/img/architecture.png)

---

## O desafio

Dados semanais e incompletos, sem fonte única.

No mercado financeiro, risco, liquidez e enquadramento dependem de dados que precisam estar
corretos e no horário. No cenário original, eles vinham de sete fontes com formatos, volumes e
janelas diferentes: bancos operacionais, uma base legada na casa dos terabytes, dados de
mercado e arquivos XML. Conciliar e gerar o relatório levava cerca de uma semana e cobria só
liquidez. Nesse contexto, surgiu a urgência de integrar um novo software regulatório,
alimentado por todas essas fontes.

O desafio de engenharia:

- integrar bancos, views legadas, um CSV diário e dois formatos de XML;
- normalizar identificadores que não batem entre sistemas;
- lidar com entregas atrasadas, corrigidas e duplicadas;
- reprocessar qualquer data com segurança, sem duplicar nada;
- impedir que dado inválido chegue ao consumidor, mantendo a última versão válida;
- rastrear cada número publicado até a entrega de origem.

## O resultado

Dados diários e confiáveis.

**No projeto original**, a plataforma reduziu em cerca de 80% o tempo de construção,
validação e entrega dos dados de liquidez e enquadramento (de uma semana para um dia) e passou
a cobrir os dois domínios, atendendo aos requisitos regulatórios no prazo. Esses números são
do contexto profissional e **não são medidos por esta reconstrução**.

**Nesta reconstrução**, o que está comprovado por testes e evidências:

- pipeline completo validado localmente, em PostgreSQL 17 + SQL Server 2022 (containers) e em
  SQLite, com **valores publicados idênticos** entre os dois ambientes;
- **8 cenários de falha** reproduzíveis (duplicidade, XML inválido, atraso, correção, falha de
  qualidade, fonte indisponível, falha no meio da publicação, publicadores concorrentes),
  todos com o comportamento esperado;
- infraestrutura AWS provisionada e destruída com Terraform, com evidências.

O status exato de cada componente está em [Status de validação](#status-de-validação).

## A solução

Lakehouse medalhão na AWS.

As fontes são agrupadas por **padrão de ingestão**, não uma a uma:

| No diagrama | No código |
|---|---|
| Aurora, PostgreSQL e SQL Server de liquidez → Glue job (volume em GB) | `glue/extract-liquidez/` |
| SQL Server de enquadramento → Glue job (volume em TB) | `glue/extract-enquadramento/` |
| B3 (dados de mercado, CSV diário) → Lambda | `lambda/market-data/` |
| XML internos → Lambda | `lambda/internal-xml/` |
| XML externos → Lambda | `lambda/external-xml/` |
| Bronze, Silver e Gold com dbt sobre Athena + Iceberg | `dbt/` |
| Gold → Glue job · Load → SQL Server de controle e relatórios | `glue/export-risk-analytics/` + `apps/publisher/` |
| Orquestração | `airflow/dags/erl_pipeline.py` |
| CloudWatch + SNS, infra via Terraform | `infra/` |

Fontes de dados (stand-ins sintéticos): `apps/source-generator/`. Extração SQL e CLI:
`apps/hybrid-runner/`.

### Decisões de engenharia

- **Ingestão por padrão.** Três padrões cobrem as sete fontes. Uma fonte nova entra
  reaproveitando um padrão, sem código novo do zero.
- **Medalhão com Iceberg.** A Bronze guarda o dado como chegou (append-only, com manifestos e
  checksums), permitindo auditar e recalcular qualquer dia.
- **Qualidade como portão.** Testes do dbt rodam sobre candidatos (write-audit-publish) e
  bloqueiam a promoção entre camadas. Dado inválido não chega ao consumidor: fica em
  quarentena para análise.
- **Operação previsível.** Reprocessamento idempotente por data de referência, com
  checkpoints por etapa e alerta da etapa exata que falhou.
- **Publicação transacional.** A carga no SQL Server usa staging, validação no banco, uma
  única transação, idempotência por hash de conteúdo e trava de publicação. Se algo falha, a
  versão anterior continua intacta.
- **Um pacote entre a Gold e o SQL Server.** A AWS não se conecta a um banco local (sem VPN
  nem NAT). O Glue exporta a Gold auditada para o S3 e o publisher local faz a carga. Em uma
  rede corporativa, o mesmo job escreveria via conexão JDBC do Glue.

## Status de validação

| Componente | Status |
|---|---|
| 7 fontes, extração, Bronze, quarentena | ✅ Validado localmente (PostgreSQL + SQL Server e SQLite) |
| Silver e Gold (dbt) e portão de qualidade | ✅ Validado localmente em DuckDB · ⚠️ Athena/Iceberg não validados na AWS |
| Publicação transacional no SQL Server | ✅ Validado localmente, inclusive com publicadores concorrentes |
| Airflow | ✅ Validado localmente (`airflow dags test`, sucesso e falha de qualidade) |
| Lambdas | ✅ Pacotes testados localmente · ⚠️ a única execução na AWS falhou (codec Zstandard); a correção não foi validada na AWS |
| Glue jobs | ⚠️ Implementados, não executados (sem Spark local e sem execução na AWS) |
| CloudWatch + SNS | ⚠️ Logs reais e alarme para o SNS observados; demais caminhos de alerta não verificados |
| Terraform | ✅ Checks locais (fmt, validate, testes, TFLint, Checkov) · infraestrutura anterior aplicada e destruída; o layout atual nunca foi aplicado |

Os recursos de AWS foram removidos após a validação. Detalhes: [docs/AWS_VALIDATION.md](docs/AWS_VALIDATION.md)
e [evidence/aws/](evidence/aws/).

## Como rodar

Requisitos: [uv](https://docs.astral.sh/uv/) e Python 3.12 (o uv instala). Docker é opcional
([docs/LOCAL_RUNTIME.md](docs/LOCAL_RUNTIME.md)).

```bash
uv sync                                 # dependências travadas (uv.lock)
cp .env.example .env                    # padrão: ERL_MODE=local, ERL_SOURCE_BACKEND=sqlite
uv run erl generate                     # 4 bancos de origem + arquivos de 2026-10-26 a 28
uv run erl run --date 2026-10-26        # pipeline completo; mostra run id, etapas e relatório
uv run erl publications                 # log de publicações no consumidor
```

Airflow (venv separado, metadados em SQLite):

```bash
scripts/airflow_local.sh setup && scripts/airflow_local.sh test 2026-10-26
```

Testes:

```bash
uv run ruff check apps protocols contracts tests airflow glue lambda scripts
uv run pytest -m "not dbt and not integration"
DBT_SEND_ANONYMOUS_USAGE_STATS=false uv run pytest -m "dbt and not integration"
scripts/terraform_checks.sh             # sem credenciais
```

## Cenários de falha e reprocessamento

Reproduza todos com `uv run python scripts/collect_evidence.py`.

| Cenário | Comportamento observado |
|---|---|
| Identificadores divergentes, duplicidades, carteira sem mapeamento | resolvidos via crosswalk; rejeições registradas em `silver_rejected_records` |
| XML malformado ou fora do XSD | arquivo em quarentena com `.rejection.json`; aviso de completude |
| Data de referência atrasada | entra na partição correta; o reprocessamento publica nova versão e a anterior vira `SUPERSEDED` |
| Reexecução da mesma carga | snapshots `DUPLICATE`; publicação `SKIPPED_IDENTICAL` |
| Correção de posição publicada | nova versão publicada; reconciliação sinaliza a divergência |
| Falha crítica de qualidade | portão bloqueia; Gold intocada; consumidor mantém a última versão válida |
| Fonte indisponível | `source_unavailable`, código de saída 3, relatório gerado |
| Falha no meio da publicação | transação desfeita; versão anterior intacta; log `FAILED` |
| Publicadores concorrentes | serializados por trava; exatamente uma versão `ACTIVE` |

## Estrutura do repositório

| Caminho | Responsabilidade |
|---|---|
| `apps/hybrid-runner/` | CLI, configuração, checkpoints, extração SQL e orquestração local |
| `apps/publisher/` | validação do pacote e publicação transacional |
| `apps/source-generator/` | sistemas de origem sintéticos e cenários |
| `lambda/` | uma pasta por função, com handler, parser, dependências e ZIP próprios |
| `glue/` | uma pasta por job, com entry point, Spark e artefatos próprios |
| `dbt/` | staging, Silver, Gold e testes |
| `airflow/` | DAG do pipeline |
| `infra/` | Terraform (bootstrap, ambiente dev e módulos) |
| `contracts/` | contratos JSON, XSD e schemas de origem |
| `protocols/integrity-v2/` | protocolo de integridade versionado |
| `evidence/` | evidências locais e de AWS (sanitizadas) |

## Limitações

- PostgreSQL e SQL Server rodam como containers locais, não como instâncias gerenciadas.
- Glue, Athena e Iceberg não foram executados na AWS; nenhum produto Gold foi publicado a
  partir da AWS.
- Volumes pequenos por design: não há afirmações de performance.
- As regras de liquidez e concentração são demonstrações sintéticas, não cálculos regulatórios.

Lista completa: [docs/HANDOFF.md](docs/HANDOFF.md) e [docs/INDEPENDENT_REVIEW.md](docs/INDEPENDENT_REVIEW.md).

## Documentação

- Modelo de dados, fontes e regras: [docs/DATA_MODEL.md](docs/DATA_MODEL.md)
- Organização do código: [docs/REFACTORING.md](docs/REFACTORING.md)
- Terraform, segurança e custos: [docs/TERRAFORM.md](docs/TERRAFORM.md)
- Execução na AWS e teardown: [docs/AWS_VALIDATION.md](docs/AWS_VALIDATION.md)
- Acordos de trabalho para pessoas e agentes: [AGENTS.md](AGENTS.md)

---

Feito por **Anderson Nascimento** · [Portfólio](https://anderson-adm.github.io/data-engineer-portfolio-site/) · [LinkedIn](https://www.linkedin.com/in/anderson-santos-do-nascimento)
