# flink-anomaly-job

Kafka(`fab-semiconductor-001`)의 센서 데이터를 **하나의 Flink 잡**에서 받아, 소스 1번 읽고
**flatten을 한 번만** 계산한 뒤 **세 갈래(task 분기)** 로 처리한다. 각 분기는 slot sharing group으로
**서로 다른 TaskManager에 분산** 배치된다.

```
Kafka(fab-semiconductor-001)
   │  flatMap → SensorRecord   (flatten 1번, 모든 분기 공유)
   ├─[s3]    keyBy(dedupKey) → dedup → Parquet → S3            (cold/분석)
   ├─[os]    → OpenSearch (sensor-realtime, raw 측정값)         (실시간/Grafana)
   └─[anom]  map→ReadingEvent → watermark → 이상감지 → Kafka(sensor-violations)
```

`flink-s3-job` + `flink-anomaly-job` + OpenSearch 적재를 **한 잡으로 통합**한 형태다.
(나중에 하드 격리가 필요하면 분기를 각각 Application 모드 잡으로 분리)

---

## 왜 한 잡인가 (설계 의도)

- **공유 효율**: 소스 읽기·flatten을 **한 번만** 하고 세 분기가 재사용 → 별개 잡 대비 중복 읽기/연산 없음.
- **task 분산**: `slotSharingGroup("s3"/"os"/"anom")` 로 분기를 다른 TaskManager pod에 배치 → 자원 격리.
- **트레이드오프**: 체크포인트·장애 도메인은 공유(한 잡). 이를 분기별 방어 코드 + 멱등 싱크로 완화.
  스트리밍은 단일 pipelined region이라 **uncaught 예외는 잡 전체 재시작** — 그래서 각 싱크에 실패 처리를 둔다.

---

## 중복·지연·누락 처리 (저장소별 최적 방식)

| | S3 분기 | OpenSearch 분기 | 이상감지 분기 |
|---|---|---|---|
| **중복** | dedup 연산자(TTL keyed state) + exactly-once 파일 커밋 | **결정적 `_id` upsert**(멱등) | (해당 없음 — 이벤트 평가) |
| **지연(out-of-order)** | event-time(`measuredAt`) 파티셔닝 → 늦어도 맞는 `dt=` 파티션 | 순서 무관 — `_id` upsert | watermark(70s 허용) + 5분 윈도우 |
| **누락** | 체크포인트 후 offset 커밋(at-least-once→exactly-once) | at-least-once + 체크포인트 flush, poison→FailureHandler skip | 체크포인트, Kafka 싱크 |

→ S3는 "파일 exactly-once + dedup", OpenSearch는 "멱등 `_id` upsert + at-least-once" — 각 싱크 특성에 맞춤.

---

## 모듈 구조

```
flink-unified-job/
├── src/main/java/com/factory/flink/
│   ├── FlinkUnifiedJob.java              # main: 소스→flatten→3분기(slot group)
│   ├── config/JobConfig.java             # 환경변수(소스/S3/OpenSearch/violations)
│   ├── dto/                              # 공유 DTO + SensorRecord + 이상감지 DTO
│   ├── serialization/                    # 배치 역직렬화, violation 직렬화
│   ├── process/
│   │   ├── SensorRecordFlatMapFunction   # 공유 flatten
│   │   ├── DeduplicationProcessFunction  # S3 dedup
│   │   ├── SensorRecordToReadingEvent    # 이상감지용 경량 projection
│   │   └── AnomalyEvaluationProcessFunction  # Nelson/Bias 룰 엔진
│   ├── sink/                             # Parquet(S3) 싱크 + Avro 변환 + 버킷
│   └── opensearch/
│       ├── SensorDocument.java           # 결정적 _id + source map (순수)
│       ├── SensorRecordEmitter.java      # SensorRecord → IndexRequest
│       └── SensorRecordOpensearchSink.java   # OpensearchSink 빌더 + FailureHandler
└── src/test/java/...
```

---

## 환경 변수 (주요)

| 변수 | 기본값 | 분기 |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | 공통 |
| `KAFKA_SOURCE_TOPIC` | `fab-semiconductor-001` | 공통 |
| `KAFKA_GROUP_ID` | `flink-unified-ingestion` | 공통 |
| `CHECKPOINT_INTERVAL_MS` | `60000` | 공통 |
| `S3_OUTPUT_PATH` | `s3a://sensor-data-lake/sensor` | S3 |
| `DEDUP_TTL_HOURS` | `6` | S3 |
| `OPENSEARCH_HOST` / `OPENSEARCH_PORT` | `opensearch` / `9200` | OpenSearch |
| `OPENSEARCH_SCHEME` | `http` | OpenSearch |
| `OPENSEARCH_INDEX` | `sensor-realtime` | OpenSearch |
| `OPENSEARCH_USERNAME` / `OPENSEARCH_PASSWORD` | (빈값=인증없음) | OpenSearch |
| `KAFKA_VIOLATIONS_TOPIC` | `sensor-violations` | 이상감지 |

> 잘못된/빈 값은 예외 없이 기본값으로 폴백(fail-soft).

---

## 빌드 / 테스트 / 커버리지

```bash
gradle test jacocoTestCoverageVerification jar      # 테스트 + 80% line/branch 게이트 + fat jar
```

- **기능 테스트**: flatten, dedup(harness), Avro/Parquet 변환, 버킷, OpenSearch `_id`/문서/emitter, 이상감지 룰, violation 직렬화
- **성능 테스트**: `perf/IngestionThroughputTest` — flatten+문서매핑 처리량 하한 검증
- **커버리지**: jacoco **line·branch ≥80%** 게이트. 클러스터 없이는 검증 불가한 결선부
  (`FlinkUnifiedJob`, `SensorRecordParquetSink`, `SensorRecordOpensearchSink`)는 분모에서 제외.
- Lombok 생성 코드는 `lombok.config`의 `@Generated`로 제외.

---

## 보안 검토

| 항목 | 결과 |
|---|---|
| 경로 traversal(equipmentId) | `EquipmentDateBucketAssigner.sanitize()` 방어 |
| Jackson 역직렬화 RCE | 폴리모픽 타이핑 없음, 고정 POJO 바인딩 |
| JSON DoS | Jackson 2.15 StreamReadConstraints |
| 하드코딩 시크릿 | 없음 — OpenSearch 자격증명은 env(운영은 Secret/IRSA) |
| poison 입력 가용성 | 역직렬화 skip + OpenSearch FailureHandler skip + dedup TTL |
| 명령 주입 | 없음 |

운영 권고: OpenSearch는 **TLS + 인증(Secret 주입)**, S3는 **IRSA**, Kafka는 **TLS/SASL**.

---

## 배포 (Flink Kubernetes Operator, Application 모드)

`k8s/app/flink-anomaly-job-flinkdeployment.yml` — 한 FlinkDeployment(=한 잡). TaskManager 병렬도/슬롯을
늘리면 세 분기가 서로 다른 TM pod에 분산된다. 자세한 로컬 실행은 루트 `local-infra/` 참고.
