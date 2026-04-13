# Cloud Pricing Data Lake — AWS Architecture

**Status:** Draft
**Data:** 2026-04-12
**Autor:** @architect (Aria)
**Base:** `docs/architecture/cloud-pricing-data-lake.md`
**Próximo:** @data-engineer — pipeline Python por cloud

---

## Diagrama de Arquitetura AWS

```
┌─────────────────────────────────────────────────────────────────────────┐
│  AWS ACCOUNT — Cloud Pricing Data Lake                                  │
│                                                                         │
│  ┌─────────────────┐                                                    │
│  │  EventBridge    │  cron: "0 21 * * *" (21:00 UTC = 18:00 BRT diário)             │
│  │  (Scheduler)    │                                                    │
│  └────────┬────────┘                                                    │
│           │ dispara                                                     │
│           ▼                                                             │
│  ┌─────────────────────────────────────────────────────────┐           │
│  │  AWS Batch                                              │           │
│  │  Compute: Fargate | vCPU: 2 | Memory: 4GB              │           │
│  │  Image: ECR → pricing-lake-ingestion:latest             │           │
│  │  Retry: 2x | Timeout: 2h                                │           │
│  └───────────────────────────┬─────────────────────────────┘           │
│                              │                                         │
│  ┌───────────────────────────▼─────────────────────────────┐           │
│  │  PIPELINE PYTHON (container)                            │           │
│  │                                                         │           │
│  │  Scraper AWS · Scraper Azure · Scraper GCP · Oracle     │           │
│  │       └──────────────────────────────────┘             │           │
│  │                    Orquestrador (main.py)               │           │
│  │          Fase 1: Raw → Fase 2: Consolidar → Fase 3: Parquet         │
│  └───────────────────────────┬─────────────────────────────┘           │
│                              │                                         │
│  ┌───────────────────────────▼─────────────────────────────┐           │
│  │  Amazon S3 — bucket: pricing-lake                       │           │
│  │  raw/          → Standard-IA → Glacier (30d)           │           │
│  │  consolidated/ → Standard-IA → Glacier (30d)           │           │
│  │  parquet/      → Standard (permanente)                 │           │
│  └──────┬────────────────────┬────────────────────┬────────┘           │
│         │                   │                    │                     │
│  ┌──────▼──────┐  ┌─────────▼──────┐  ┌─────────▼────────┐           │
│  │  AWS Glue   │  │  Amazon Athena │  │  CloudWatch      │           │
│  │  Data Catalog│  │  (SQL queries) │  │  Logs + Alarms   │           │
│  └─────────────┘  └────────────────┘  └─────────┬────────┘           │
│                                                  │ SNS                 │
│                                            Email / Slack               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## IAM Roles & Políticas

### Role 1 — `pricing-lake-batch-role`
*Usada pelo container AWS Batch durante execução*

```json
{
  "RoleName": "pricing-lake-batch-role",
  "AssumedBy": "ecs-tasks.amazonaws.com",
  "Policies": [
    {
      "Name": "S3Access",
      "Actions": ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::pricing-lake", "arn:aws:s3:::pricing-lake/*"]
    },
    {
      "Name": "GlueAccess",
      "Actions": [
        "glue:CreateTable", "glue:UpdateTable", "glue:GetTable",
        "glue:BatchCreatePartition", "glue:CreatePartition", "glue:UpdatePartition"
      ],
      "Resource": "arn:aws:glue:*:*:*"
    },
    {
      "Name": "SecretsManager",
      "Actions": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:*:*:secret:pricing-lake/*"
    },
    {
      "Name": "CloudWatchLogs",
      "Actions": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Name": "CloudWatchMetrics",
      "Actions": ["cloudwatch:PutMetricData"],
      "Resource": "*"
    }
  ]
}
```

### Role 2 — `pricing-lake-eventbridge-role`
*Usada pelo EventBridge para disparar o Batch job*

```json
{
  "RoleName": "pricing-lake-eventbridge-role",
  "AssumedBy": "events.amazonaws.com",
  "Policies": [
    {
      "Name": "BatchSubmit",
      "Actions": ["batch:SubmitJob"],
      "Resource": "arn:aws:batch:*:*:job-definition/pricing-lake-*"
    }
  ]
}
```

### Role 3 — `pricing-lake-analyst-role`
*Para humanos/ferramentas que consultam via Athena*

```json
{
  "RoleName": "pricing-lake-analyst-role",
  "AssumedBy": "iam.amazonaws.com",
  "Policies": [
    {
      "Name": "AthenaQuery",
      "Actions": [
        "athena:StartQueryExecution", "athena:GetQueryResults",
        "athena:GetQueryExecution", "athena:GetWorkGroup"
      ],
      "Resource": "*"
    },
    {
      "Name": "S3ReadParquet",
      "Actions": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::pricing-lake/parquet/*"]
    },
    {
      "Name": "GlueRead",
      "Actions": ["glue:GetTable", "glue:GetPartitions", "glue:GetDatabase"],
      "Resource": "*"
    }
  ]
}
```

---

## AWS Batch — Compute Environment

```yaml
ComputeEnvironment:
  name: pricing-lake-compute
  type: MANAGED
  computeResources:
    type: FARGATE
    maxvCpus: 4
    subnets: [subnet-xxxxxxxxx]        # subnet privada
    securityGroupIds: [sg-xxxxxxxxx]   # egress HTTPS only

JobQueue:
  name: pricing-lake-queue
  priority: 1
  computeEnvironments: [pricing-lake-compute]

JobDefinition:
  name: pricing-lake-ingestion
  type: container
  platformCapabilities: [FARGATE]
  containerProperties:
    image: "{account}.dkr.ecr.{region}.amazonaws.com/pricing-lake-ingestion:latest"
    command: ["python", "main.py", "--date", "Ref::date"]
    resourceRequirements:
      - {type: VCPU, value: "2"}
      - {type: MEMORY, value: "4096"}
    jobRoleArn: "arn:aws:iam::{account}:role/pricing-lake-batch-role"
    logConfiguration:
      logDriver: awslogs
      options:
        awslogs-group: /aws/batch/pricing-lake
        awslogs-region: us-east-1
  retryStrategy:
    attempts: 2
    evaluateOnExit:
      - onStatusReason: "Host EC2*terminated"
        action: RETRY
      - onReason: "*"
        action: FAILED
  timeout:
    attemptDurationSeconds: 7200   # 2 horas — tempo total do job completo
                                   # (download + consolidação + parquet)
                                   # Retry recomeça do zero (Fase 1)
```

---

## Estratégia de Tratamento de Falhas

### 3 Níveis de Proteção

**Nível 1 — Por scraper (dentro do container)**
- Retry individual por cloud: 3 tentativas
- Backoff exponencial: 30s → 60s → 120s
- Rate limit 429 → respeita header `X-RateLimit-Reset`
- Falha de 1 cloud NÃO cancela as demais
- Resultado parcial é salvo com log de erro

**Nível 2 — Por job AWS Batch**
- Retry automático: 2 tentativas
- Timeout: 2 horas (protege contra hang)
- Fargate interruption → retry automático

**Nível 3 — Alertas e observabilidade**
- CloudWatch Alarm: job FAILED → SNS → email
- CloudWatch Alarm: job > 60min → alerta warning
- Log estruturado JSON por fase e cloud
- Métricas customizadas: SKUs coletados por cloud/dia

### Tabela de Cenários

| Cenário | Resposta |
|---------|---------|
| API retorna 429 | Backoff + retry (3x) |
| API retorna 503 | Backoff + retry (3x) |
| Schema da API mudou | Falha controlada + alerta SNS |
| S3 PutObject falhou | Retry automático (boto3) |
| Container timeout >2h | Batch cancela + alerta crítico |
| Fargate interruption | Batch reinicia automaticamente |
| 1 cloud falhou, 3 ok | Salva parcial + alerta warning |
| Todas as clouds falharam | Alerta crítico SNS |

---

## Estrutura do Projeto Python

```
pricing-lake-ingestion/
├── Dockerfile
├── requirements.txt
├── main.py                    # orquestrador principal
│
├── scrapers/
│   ├── base.py               # classe base abstrata
│   ├── aws.py                # AWS Pricing API (~500 serviços)
│   ├── azure.py              # Azure Retail Prices API (paginada)
│   ├── gcp.py                # GCP Cloud Billing Catalog
│   └── oracle.py             # Oracle Price List
│
├── pipeline/
│   ├── consolidator.py       # Camada 1 → 2 (merge por provider)
│   └── transformer.py        # Camada 2 → 3 (Parquet canônico)
│
├── storage/
│   └── s3.py                 # upload, paths, lifecycle
│
├── catalog/
│   └── glue.py               # registrar partições no Glue Catalog
│
└── utils/
    ├── logger.py             # JSON structured logging
    ├── metrics.py            # CloudWatch custom metrics
    └── retry.py              # backoff exponencial
```

---

## AWS Glue — DDL Athena

```sql
CREATE EXTERNAL TABLE pricing_lake.prices (
  provider        STRING,
  sku             STRING,
  service         STRING,
  service_family  STRING,
  description     STRING,
  region          STRING,
  region_orig     STRING,
  geography       STRING,
  price_usd       DOUBLE,
  price_orig      DOUBLE,
  currency_orig   STRING,
  unit            STRING,
  unit_orig       STRING,
  price_type      STRING,
  effective_date  STRING,
  collected_date  STRING,
  source_file     STRING
)
PARTITIONED BY (date STRING, provider STRING)
STORED AS PARQUET
LOCATION 's3://pricing-lake/parquet/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

---

## CloudWatch — Alertas

```yaml
Alarms:
  - Name: pricing-lake-job-failed
    MetricName: FailedJobCount
    Namespace: AWS/Batch
    Threshold: 1
    Actions: [SNS → email/slack]

  - Name: pricing-lake-job-slow
    MetricName: ElapsedTime
    Namespace: pricing-lake/custom
    Threshold: 3600000   # 60 min
    Actions: [SNS → warning email]

  - Name: pricing-lake-partial-failure
    MetricName: CloudsFailedCount
    Namespace: pricing-lake/custom
    Threshold: 1
    Actions: [SNS → warning email]
```

---

## Segurança

| Camada | Controle |
|--------|---------|
| S3 bucket | Block Public Access habilitado |
| S3 bucket | Server-side encryption (SSE-S3) |
| IAM | Least privilege por role |
| Batch container | Credenciais via IAM role (sem hardcode) |
| ECR | Scan automático de vulnerabilidades |
| Networking | Subnet privada, sem IP público |
| API keys externas | AWS Secrets Manager (`pricing-lake/api-keys`) |

---

## Decisões Arquiteturais

| Decisão | Escolha | Rationale |
|---------|---------|-----------|
| Compute | Fargate | Sem gerenciamento de instância |
| Estrutura do job | Script único (1 job, 3 fases sequenciais) | Simples de operar; retry recomeça da Fase 1 |
| Paralelismo | Scrapers sequenciais dentro do script | Simples; 2h suficiente para MVP |
| Timeout | 2h para o job completo (não por fase) | Cobre download + consolidação + parquet |
| Compressão Parquet | SNAPPY | Melhor trade-off velocidade/tamanho para Athena |
| Particionamento Glue | date + provider | Alinha com S3, queries por data e cloud |
| Networking | Subnet privada | Container acessa S3 via VPC endpoint |
| Secrets | Secrets Manager | API keys externas com rotação automática |

---

## Próximos Passos — @data-engineer

- [ ] Implementar `scrapers/aws.py` (AWS Pricing API — ~500 serviços)
- [ ] Implementar `scrapers/azure.py` (paginação nextLink)
- [ ] Implementar `scrapers/gcp.py` (Cloud Billing Catalog)
- [ ] Implementar `scrapers/oracle.py` (por família de produto)
- [ ] Implementar `pipeline/consolidator.py`
- [ ] Implementar `pipeline/transformer.py` (→ schema canônico 17 campos)
- [ ] Criar `Dockerfile` + `requirements.txt`
- [ ] Configurar `utils/retry.py` com backoff exponencial

---

*Documento gerado por @architect (Aria) — 2026-04-12*
*Próximo handoff: @data-engineer*
