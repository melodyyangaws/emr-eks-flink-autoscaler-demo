# Streaming Analytics: Engine Team Review Session
### Cross-reference: SIFT Growth Divergence | 2026 Streaming CLF | Paimon vs Iceberg Benchmark
**Presenter:** Melody Yang (meloyang) — WW SSA, Streaming Analytics
**Date:** May 2026 | **Audience:** Spark Engine, Flink Engine, Open Analytics PMs/SDMs

---

## Slide 1: Why We're Here

Three independent data sources are telling the same story:

| Source | Signal |
|---|---|
| **SIFT Field Insight** (Apr 24) | MSF revenue +44.8% YoY vs EMR -33.7% YTD — structural shift to streaming |
| **2026 Streaming CLF** (Mar 2026) | 15+ customer interviews, $600K+ confirmed ARR, $12M+ at risk, 78 PFRs on CDC alone |
| **Paimon vs Iceberg Benchmark** (Mar 2026) | Kafka-less CDC architecture is 5.4× cheaper — Databricks ZeroBus validates same pattern |

**The ask:** Align on engineering priorities before VP review (target: May 30, 2026)

---

## Slide 2: The Revenue Picture

### MSF Is Growing Where EMR Is Declining

| Metric | MSF (Flink) | EMR (Spark) | Source |
|---|---|---|---|
| YTD YoY growth | **+44.8%** | **-33.7%** | GCR Analytics Revenue Report |
| Trend consistency | Growing across APJ + NAMER | Declining across all geos | SIFT insight |
| EMR provisioned churn | — | ~1/3 → Serverless, rest → Databricks or efficiency gains | Apr 21 Analytics WBR |
| Redshift Serverless NRR | — | Dropped 150% → 128% | Apr 21 WBR 6th block |
| Top churning customers | — | Cerner, Twitter — departing entirely | WBR deep dive |

**Takeaway:** This is not a product-level miss. It's an engine-level shift. Customers are choosing streaming-first architectures and never evaluating batch.

---

## Slide 3: Five Customer Proof Points (SIFT)

| Customer | Pattern | Revenue | Key Detail |
|---|---|---|---|
| **Rippling** | Flink-first, consolidated MSK | $3M/yr Flink | 30 → 12 MSK clusters; evaluating blue-green Flink deploys; never considered EMR |
| **TopGolf** | Real-time gaming pipeline | Net-new | MSK → S3 Tables via Firehose; 30-min freshness replacing 24-48hr batch; EMR not evaluated |
| **Allegiant Travel** | Glue Streaming at scale | $11.2M ARR | 1,800 concurrent streams; active escalation for autoscaling bugs; won't revert to batch |
| **Deloitte GES Tax** | Streaming-by-default | $38K/yr per job | 24/7 Glue Streaming for simple MSK→S3; could save 90% with Firehose; chose streaming anyway |
| **Boston Scientific** | Industrial IoT streaming | — | Manufacturing Execution System real-time pipeline |

**Pattern:** Even when streaming is the wrong choice (Deloitte), customers still default to it. Streaming-first is the new baseline.

---

## Slide 4: Why — Three Structural Forces

### 1. AI Demands Low-Latency Data
- Real-time feature stores, streaming CDC for vector DB refresh, agentic AI architectures
- Batch windows incompatible with AI SLAs
- Gartner: 33% of enterprise apps adopt agentic AI by 2028 — each needs streaming foundations

### 2. Managed Services Absorb Complexity
- Customers choose MSF over self-managed Spark-on-EMR
- Flink: exactly-once semantics, built-in watermarking, managed scaling — zero config
- Swiggy case (Apr 9): 2-6× slower EMR vs Databricks traced to untuned configs, not engine limitations
- **Customers who cannot tune do not choose EMR**

### 3. Streaming-First Is the New Default
- Rippling & TopGolf architect streaming from day one
- Batch addressable market is contracting; streaming is expanding
- Databricks ZeroBus GA confirms industry direction

---

## Slide 5: CLF Evidence — What Customers Are Telling Us

### 5 Observations from 15+ Interviews (CLF Doc)

| Obs | Problem | Revenue Signal | PFRs |
|---|---|---|---|
| **#1** Fragmented CDC-to-Lakehouse | 4+ services to assemble a CDC pipeline | $9.21M at risk | 78 |
| **#2** Autoscaler cost inefficiency | Spark DRA broken; Flink autoscaler 2-5 min delay; 40-60% over-provisioning | $4.62M at risk | 30 |
| **#3** No EMR Flink runtime value | Robotaxi: zero perf delta EMR vs OSS Flink → chose self-managed for Paimon access | $4.38M at risk | 37 |
| **#4** OTF streaming gaps | Firehose small files, equality deletes, MSK connector gaps, no Flink-Redshift connector | $2.08M at risk | 24 |
| **#5** No streaming data lineage | No lineage tracking across streaming pipelines | $30M+ (3 customers) | 8 |

**Total: $12M+ confirmed revenue at risk, 177 PFRs**

---

## Slide 6: CLF Deep Dive — Key Customer Losses & Risks

| Customer | ARR | What Happened | Root Cause |
|---|---|---|---|
| **Robotaxi / Baidu** | $4.38M | Chose OSS Flink over EMR | Zero EMR perf advantage + needed Paimon (Flink 2.0) |
| **YunExpress** | — | Chose Alibaba Cloud over AWS | No unified CDC experience |
| **DHgate** | $102K | Built 4-service pipeline workaround | No native Flink-Redshift connector |
| **Leapmotor** | — | Abandoned Redshift for Doris on EC2 | No Flink-Redshift connector |
| **Docebo** | $240K | 7,300 schemas, needs schema evolution | EMR CDC doesn't support it |
| **AT&T** | $600K | Timer-based processing, not true streaming | Autoscaler limitations |
| **Swiggy** | — | 2-6× slower EMR vs Databricks | Untuned configs; customers who can't tune leave |

---

## Slide 7: The Paimon Opportunity — Benchmark Data

### Kafka-Less CDC Architecture: 5.4× Cheaper

8-hour continuous CDC benchmark: MySQL → Flink CDC → Paimon/Iceberg on EMR on EKS 7.12

| Metric | Paimon | Iceberg | Delta |
|---|---|---|---|
| Throughput | ~3,000 rec/s | ~3,000 rec/s | Equivalent |
| Data files | 797 | 8,596 + 9,928 deletes | **Paimon 91% fewer** |
| Warehouse size | 624 MB | 1.6 GB | **Paimon 61% smaller** |
| Checkpoint latency | ~3,000 ms | ~420 ms | Iceberg faster (tradeoff) |
| Kafka required? | **No** | Yes (for streaming reads) | **Key differentiator** |
| Monthly cost estimate | **$328** | **$1,762** (with MSK) | **5.4× cheaper** |

📄 Full benchmark: [Paimon vs Iceberg (v2)](https://quip-amazon.com/4fbhAx5RKOyB/Benchmark-for-Apache-Paimon-vs-Apache-Icebergv2)

---

## Slide 8: Architecture Comparison — Why Paimon Eliminates Kafka

### Traditional (Iceberg + MSK)
```
MySQL → Flink CDC → MSK → Flink Consumer → Iceberg (S3) → Athena/Redshift
         4+ services        $1,400/mo MSK alone
```

### Streaming Lakehouse (Paimon)
```
MySQL → Flink CDC → Paimon (S3) → Streaming Consumer → Athena/StarRocks
         2 services          $0 message bus cost
```

### Databricks ZeroBus (competitor)
```
Database → ZeroBus → Delta Lake (Databricks) → Databricks SQL
            Proprietary        Vendor lock-in
```

**Our advantage:** Same pattern as ZeroBus, but open-source (Flink + Paimon), runs on AWS (EMR + S3), no lock-in.

---

## Slide 9: Paimon + Iceberg = Complete Spectrum

### Not a replacement — a complement

| Workload | Best Fit | Why |
|---|---|---|
| Streaming CDC ingestion | **Paimon** | Native consumer-based streaming reads, auto-compaction, no Kafka needed |
| Real-time AI feature serving | **Paimon** | Sub-second CDC latency, changelog = training data stream |
| Batch BI / ad-hoc analytics | **Iceberg** | Broad engine support (Athena, Trino, Presto, Redshift), mature ecosystem |
| Compliance / audit queries | **Iceberg** | Time-travel, partition evolution, proven at petabyte scale |
| RAG vector DB refresh | **Paimon → Iceberg** | Paimon for hot path CDC, Iceberg compatibility for downstream batch |

**Framing: "Iceberg for batch BI. Paimon for streaming AI. Together: real-time to historical, no Kafka, 5× cheaper."**

---

## Slide 10: AI Use Cases Driving Streaming Demand

| AI Use Case | Latency Requirement | Streaming Pattern | Why Paimon Fits |
|---|---|---|---|
| **Real-time feature stores** | Sub-second | CDC → feature computation → serving | Consumer-based reads feed features continuously |
| **RAG for GenAI** | Minutes | CDC → embedding → vector DB | Changelog = incremental embedding updates |
| **Fraud detection** | Milliseconds | Transaction scoring in-stream | Sub-second CDC vs Iceberg's minutes |
| **Continuous model training** | Hours | Fresh data → retrain loop | Paimon changelog = training data stream |
| **Agentic AI orchestration** | Sub-second | Event-driven agent triggers | Streaming reads as event source |
| **Real-time recommendations** | Sub-second | User behavior → model → response | Eliminates Kafka hop in serving path |

**Gartner:** 33% of enterprise apps will adopt agentic AI by 2028.
**Every one of these use cases requires streaming data foundations.**

---

## Slide 11: Discussion — Flink Engine Team

| # | Topic | Evidence | Question for Engineering |
|---|---|---|---|
| 1 | **Flink version lag (1.20 vs 2.2)** | Robotaxi chose OSS for Flink 2.0 + Paimon | Can we commit to <3 month version adoption? |
| 2 | **Zero runtime value over OSS** | Robotaxi: identical perf EMR vs OSS | Roadmap for Flink-specific optimizations? |
| 3 | **Native Paimon support in EMR** | CLF Obs #4: Paimon+Glue fragile via Hive workaround | Ship Paimon JARs + `'metastore' = 'glue'` natively? |
| 4 | **Flink-Redshift connector** | DHgate, Leapmotor — 3 PFRs, 10 customer influences | Timeline? EMR Spark already has one |
| 5 | **Autoscaler reactive delay** | 2-5 min delay → 2-3× over-provisioning | Predictive autoscaling based on MSK queue depth? |
| 6 | **MySQL CDC for RDS 8.4+** | CLF Obs #4: current connector broken | EMR patch or OSS dependency? |
| 7 | **Paimon checkpoint contention** | Benchmark: ~50% checkpoint failures during compaction | Dedicated compaction thread pool / async compaction? |

---

## Slide 12: Discussion — Spark Engine Team

| # | Topic | Evidence | Question for Engineering |
|---|---|---|---|
| 1 | **Spark Structured Streaming DRA broken** | Unity, Appsflyer, WestPac — 40-60% waste; 12 PFRs / $4.62M | Plan for streaming-aware DRA? |
| 2 | **No streaming perf differentiator** | Multiple customers confirm zero advantage over OSS | Strategic concession or investment planned? |
| 3 | **Low-volume streaming uneconomical** | Airbus: 20K msgs/day needs 24/7 driver+executor → moved to Lambda | Cost-optimized tier / scale-to-zero? |
| 4 | **Spot on EMR Serverless** | Iron Source proved Spot works on EMR EKS | Timeline for Serverless Spot? |
| 5 | **Ship Paimon Spark connector** | EMR ships Iceberg + Hudi but not Paimon | Include in EMR 8.x? Paimon supports Spark 3.3+ |
| 6 | **Spark readStream + Paimon** | Paimon supports streaming reads in Spark 3.3+ | Certify on EMR Spark 3.5.x? |
| 7 | **Athena Spark version** | Spark 3.2 → no Paimon support (needs 3.3+) | Upgrade timeline? |

---

## Slide 13: Cross-Team / Product Asks

| # | Topic | Owner | Timeline |
|---|---|---|---|
| 1 | **Unified CDC experience in SageMaker Unified Studio** | Open Analytics PM | 2026 H2 |
| 2 | **Native Paimon table type in Glue Catalog** | Glue PM | 2026 H2 |
| 3 | **Automate EMR churn detection** | Analytics BI team | Immediate |
| 4 | **Separate EMR reporting** (EC2 / EKS / Serverless) | WBR owners | In progress |
| 5 | **Competitive positioning vs Databricks ZeroBus** | GTM / Marketing | Pre-re:Invent |
| 6 | **Public blog / re:Invent talk** | SSA + PM | Submit by July |
| 7 | **Customer validation pilot** | SSA + SA | Rippling, TopGolf candidates |

---

## Slide 14: Proposed Timeline

| Date | Milestone | Owner |
|---|---|---|
| **May 9** | Engine team review sessions (this deck) | meloyang |
| **May 16** | Engineering/PM responses for all CLF observations | PM/SDM leads |
| **May 23** | Consolidated action plan with committed priorities | meloyang + PMs |
| **May 30** | VP presentation with closed-loop actions | meloyang |
| **June** | Paimon benchmark published as architecture guide | meloyang + GTM |
| **July** | re:Invent session submission | meloyang + khazenj |
| **2026 H2** | Unified CDC experience + native Paimon in EMR | Engineering |

---

## Slide 15: The One-Line Takeaway

> **Streaming is no longer an alternative to batch — it's the default architecture for AI-era data platforms. The $12M+ revenue at risk in our CLF, the 44.8% MSF growth, and the Databricks ZeroBus launch all point the same direction. The question isn't whether to invest in streaming — it's whether we move fast enough to own the open-source streaming lakehouse before Databricks locks it down.**

---

### Reference Links
- 📄 [2026 Streaming Analytics CLF](https://quip-amazon.com/CKGHAetG7pio/Feedback-Package-2026-AWS-Streaming-Analytics)
- 📄 [Paimon vs Iceberg Benchmark](https://quip-amazon.com/4fbhAx5RKOyB/Benchmark-for-Apache-Paimon-vs-Apache-Icebergv2)
- 📊 [SIFT: Streaming Growth Divergence](https://aws-crm.lightning.force.com/lightning/n/Sales_Insights_Field_Trends?c__insightId=dd0d4401-8d2a-4b01-9091-081f68694242)
- 📰 [Databricks ZeroBus GA Announcement](https://www.databricks.com/blog/announcing-general-availability-zerobus-ingest-part-lakeflow-connect)
