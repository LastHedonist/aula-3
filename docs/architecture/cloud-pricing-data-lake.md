# Cloud Pricing Data Lake — Architecture Brief

**Status:** Draft
**Data:** 2026-04-12
**Autor:** @analyst (Atlas)
**Próximo:** @architect — design técnico AWS

---

## Visão Geral

Sistema para coleta, armazenamento e análise dos preços de lista (list price) das 4 principais cloud providers, atualizado diariamente. Implementado como um data lake de 3 camadas (Medallion Architecture) na AWS.

**Clouds cobertas:** AWS · Azure · GCP · Oracle Cloud

---

## Arquitetura — 3 Camadas (Medallion)

```
FONTES EXTERNAS
  AWS Pricing API
  Azure Retail Prices API
  GCP Cloud Billing Catalog
  Oracle Price List
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│  CAMADA 1 — RAW (Bronze)                                    │
│  s3://pricing-lake/raw/{date}/{provider}/                   │
│                                                             │
│  • Arquivo bruto original, exatamente como veio da fonte    │
│  • Preserva formato, estrutura e particionamento original   │
│  • Múltiplos arquivos por dia se a cloud entregar assim     │
│  • Imutável após ingestão                                   │
│  • Storage class: S3 Standard-IA                            │
│  • Retenção: 30 dias → move para S3 Glacier                 │
└──────────────────────┬──────────────────────────────────────┘
                       │ consolidação (merge se N > 1 arquivo)
┌──────────────────────▼──────────────────────────────────────┐
│  CAMADA 2 — CONSOLIDATED (Silver)                           │
│  s3://pricing-lake/consolidated/{date}/{provider}/          │
│                                                             │
│  • 1 arquivo único por cloud por dia                        │
│  • Mesmo conteúdo e formato da Camada 1                     │
│  • No-op se Camada 1 já tem apenas 1 arquivo (ex: GCP)      │
│  • Storage class: S3 Standard-IA                            │
│  • Retenção: 30 dias → move para S3 Glacier                 │
└──────────────────────┬──────────────────────────────────────┘
                       │ transformação + padronização
┌──────────────────────▼──────────────────────────────────────┐
│  CAMADA 3 — PARQUET (Gold)                                  │
│  s3://pricing-lake/parquet/{date}/{provider}/prices.parquet │
│                                                             │
│  • Schema canônico padronizado entre todas as clouds        │
│  • Formato Parquet para queries e análises                  │
│  • Fonte de verdade para comparação e analytics             │
│  • Storage class: S3 Standard (sempre acessível)            │
│  • Retenção: indefinida — sem lifecycle rule                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Comportamento por Cloud

| Cloud | API / Fonte | Arquivos/dia | Comportamento Camada 2 |
|-------|------------|--------------|------------------------|
| **AWS** | AWS Pricing API (JSON por serviço) | ~500+ arquivos | Merge necessário |
| **Azure** | Azure Retail Prices API (paginada) | N páginas | Merge de páginas |
| **GCP** | Cloud Billing Catalog (JSON/CSV) | 1 arquivo | No-op |
| **Oracle** | Oracle Price List (JSON/CSV por família) | N arquivos | Merge necessário |

---

## Estrutura de Paths — Exemplo

```
s3://pricing-lake/
│
├── raw/
│   └── 2026-04-12/
│       ├── aws/
│       │   ├── ec2.json
│       │   ├── s3.json
│       │   ├── rds.json
│       │   └── ...        (~500 arquivos)
│       ├── azure/
│       │   ├── page-001.json
│       │   └── page-002.json
│       ├── gcp/
│       │   └── pricing.json
│       └── oracle/
│           ├── compute.json
│           └── storage.json
│
├── consolidated/
│   └── 2026-04-12/
│       ├── aws/prices.json
│       ├── azure/prices.json
│       ├── gcp/prices.json       ← cópia direta (no-op)
│       └── oracle/prices.json
│
└── parquet/
    └── 2026-04-12/
        ├── aws/prices.parquet
        ├── azure/prices.parquet
        ├── gcp/prices.parquet
        └── oracle/prices.parquet
```

---

## Schema Canônico — Camada 3 Parquet

17 campos padronizados entre todas as clouds:

```
IDENTIFICAÇÃO
  provider        string   — "aws" | "azure" | "gcp" | "oracle"
  sku             string   — ID original do SKU na cloud fonte
  service         string   — nome do serviço (ec2, s3, compute...)
  service_family  string   — família do serviço (Compute, Storage, Database...)
  description     string   — descrição do produto/SKU

LOCALIZAÇÃO
  region          string   — região normalizada (us-east-1, eastus...)
  region_orig     string   — nome original na fonte (US East (N. Virginia)...)
  geography       string   — área geográfica (americas, europe, apac...)

PREÇO
  price_usd       double   — preço convertido para USD
  price_orig      double   — preço na moeda original da fonte
  currency_orig   string   — moeda original (USD, EUR, BRL...)
  unit            string   — unidade normalizada (hour, GB, GB-month, request...)
  unit_orig       string   — unidade original da fonte (Hrs, GB-Mo, 1M requests...)
  price_type      string   — on-demand | spot | reserved | committed

VIGÊNCIA & RASTREABILIDADE
  effective_date  string   — data de vigência do preço (YYYY-MM-DD)
  collected_date  string   — data de coleta pelo pipeline (YYYY-MM-DD)
  source_file     string   — path do arquivo de origem na Camada 1
```

---

## Stack AWS — Decisões Definidas

| Componente | Serviço AWS | Decisão |
|-----------|------------|---------|
| Storage (todas as camadas) | Amazon S3 | ✅ Definido |
| Bucket | `pricing-lake` | ✅ Definido |
| Retenção Camadas 1 e 2 | S3 Lifecycle → Glacier | ✅ 30 dias |
| Retenção Camada 3 | S3 Standard | ✅ Indefinida |
| Query engine | Amazon Athena | ✅ Definido |
| Schema registry | AWS Glue Data Catalog | ✅ Definido |
| Executor de ingestão | AWS Batch + Python | ✅ Definido |
| Scheduler | Amazon EventBridge (cron) | ✅ Definido |
| Imagem Docker | Amazon ECR | ✅ Definido |
| Monitoramento | CloudWatch Logs + Alarms | ✅ Definido |

---

## Fluxo de Execução Diário

```
21:00 UTC (18:00 BRT)
   │
   ▼
Amazon EventBridge (cron diário)
   │
   ▼
AWS Batch Job disparado
   │
   ├── Container Python inicia (via ECR)
   │
   ├── [FASE 1] Coleta Raw → Camada 1
   │     ├── Scraper AWS   → s3://pricing-lake/raw/{date}/aws/*.json
   │     ├── Scraper Azure → s3://pricing-lake/raw/{date}/azure/*.json
   │     ├── Scraper GCP   → s3://pricing-lake/raw/{date}/gcp/*.json
   │     └── Scraper Oracle → s3://pricing-lake/raw/{date}/oracle/*.json
   │
   ├── [FASE 2] Consolidação → Camada 2
   │     ├── AWS: merge ~500 arquivos → prices.json
   │     ├── Azure: merge páginas → prices.json
   │     ├── GCP: no-op (cópia direta)
   │     └── Oracle: merge por família → prices.json
   │
   ├── [FASE 3] Transformação → Camada 3
   │     ├── Aplicar schema canônico (17 campos)
   │     ├── Normalizar unidades e moedas
   │     └── Exportar Parquet por provider
   │
   └── VM desligada automaticamente pelo AWS Batch
```

---

## S3 Lifecycle Rules

```json
[
  {
    "ID": "raw-to-glacier",
    "Prefix": "raw/",
    "Transitions": [{ "Days": 30, "StorageClass": "GLACIER" }]
  },
  {
    "ID": "consolidated-to-glacier",
    "Prefix": "consolidated/",
    "Transitions": [{ "Days": 30, "StorageClass": "GLACIER" }]
  }
]
```

---

## Estimativa de Custo (referência)

| Componente | Estimativa/mês |
|-----------|----------------|
| S3 Standard (Camada 3, ~30 dias acumulados) | ~$1-5 |
| S3 Standard-IA (Camadas 1 e 2, 30 dias) | ~$0.50-2 |
| S3 Glacier (histórico) | ~$0.10-0.50/mês |
| AWS Batch compute (~20min/dia) | ~$1-3 |
| Amazon Athena (queries ad-hoc) | $5/TB escaneado |
| **Total estimado** | **~$5-15/mês** |

---

## Pendências & Próximos Passos

### Para @architect
- [ ] Diagrama de arquitetura AWS completo (VPC, IAM, networking)
- [ ] Definir IAM roles e políticas de acesso por componente
- [ ] Configurar AWS Batch compute environment (instância EC2 recomendada)
- [ ] Estratégia de tratamento de falhas (retry, dead-letter, alertas)

### Para @data-engineer
- [ ] Design do scraper Python por cloud (AWS, Azure, GCP, Oracle)
- [ ] Lógica de merge/consolidação Camada 1 → 2 por provider
- [ ] Lógica de transformação Camada 2 → 3 (normalização de schema)
- [ ] Tratamento de rate limits e paginação por API
- [ ] Estrutura do projeto Python + Dockerfile

### Para @pm (se virar produto)
- [ ] PRD com casos de uso e usuários-alvo
- [ ] Roadmap: MVP → API pública → Dashboard → SDK

---

## Riscos Identificados

| Risco | Probabilidade | Impacto | Mitigação |
|-------|--------------|---------|-----------|
| APIs das clouds mudam sem aviso | Alta | Alto | Versionamento de scrapers, alertas de schema change |
| Rate limits nas APIs | Alta | Médio | Retry com backoff exponencial |
| AWS ~500 arquivos pode demorar | Média | Médio | AWS Batch sem timeout, paralelização |
| Preços Gov/China não expostos via API | Alta | Baixo | Documentar gap, crowdsourcing futuro |
| Custo de egress ao consultar S3 | Baixa | Baixo | Usar Athena (query in-region) |

---

*Documento gerado por @analyst (Atlas) — 2026-04-12*
*Próximo handoff recomendado: @architect*
