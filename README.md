# RocksDict Kafka Checkpoint Consumer

這是一個 Python consumer 範例，使用：

- Python `3.12`（Dockerfile）／`3.14.4`（本機開發 pyproject.toml）
- `rocksdict==0.3.29`
- `confluent-kafka==2.14.0`

Consumer 會關閉 Kafka `enable.auto.commit`，每筆 Kafka message 處理完成後先暫存在記憶體，並依照「每 N 筆」或「每 X 秒」flush 到 RocksDB。RocksDB checkpoint 內容包含下一次要讀的 offset 和中間狀態。

## 架構重點

RocksDB 在這裡負責：

- Offset 管理：記錄每個 `topic + partition` 已安全處理完成後的 `next_offset`
- Checkpoint：每 N 筆或每 X 秒保存安全點
- 中間狀態：保存 processing function 回傳的 state

多個 pod 讀同一個 topic 時，請使用相同 `group.id`。Kafka 會把同一個 consumer group 內的 partition 分配給不同 pod；同一個 partition 同時間只會由其中一個 pod 消費。

重要限制：如果 RocksDB 放在 pod 本機 ephemeral disk，partition rebalance 到另一個 pod 時，那個 pod 可能沒有該 partition 的 RocksDB checkpoint。正式環境要避免這個問題，通常會選其中一種：

- 每個 pod 掛 persistent volume，並設計 partition 與 state 的搬移策略
- 保留 RocksDB 作為本機 state store，同時設定 `COMMIT_KAFKA_OFFSETS=true`，在 RocksDB checkpoint flush 成功後才手動 commit Kafka offset，讓 consumer group rebalance 有共同 offset 可用
- 使用外部共享 state store 保存 checkpoint

此範例預設 `COMMIT_KAFKA_OFFSETS=false`，符合「不依賴 Kafka auto.commit」。要支援多 pod rebalance 更穩定，建議在部署時改為 `true`；這仍然是手動 commit，不是 auto commit。

## 專案結構

```
model-stearming-code-pvc/code/       # container 內掛載至 /app
├── common/                           # 共用框架層
│   ├── base.py                       # ProcessorProtocol 介面定義
│   ├── config.py                     # Kafka / RocksDB 設定（從環境變數讀取）
│   ├── consumer.py                   # Kafka poll + RocksDB checkpoint 核心邏輯
│   └── state.py                      # RocksDB state store（PartitionState / RocksCheckpointStore）
│
├── service/                          # 服務啟動層
│   └── app.py                        # 入口：讀 CONSUMER_PROCESS 動態載入對應 processing
│
└── model_scenarios/                  # 各業務場景（每個目錄為獨立專案）
    ├── SL0001/
    │   └── processing.py             # 信用卡即時推播邏輯，實作 ProcessorProtocol
    └── streaming/
        └── SCXXXXX/                  # 新場景範本（複製此目錄作為起點）
            ├── app.py
            └── processing.py
```

## 安裝（本機開發）

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

安裝後可直接執行：

```powershell
$env:CONSUMER_PROCESS="SL0001"
consumer
```

或不安裝直接跑：

```powershell
cd model-stearming-code-pvc/code
python -m service.app
```

## 環境變數

| 變數 | 說明 | 預設值 |
|------|------|--------|
| `CONSUMER_PROCESS` | 要載入的 model_scenarios 目錄名稱 | **必填** |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker 位址 | `localhost:9092` |
| `KAFKA_GROUP_ID` | Consumer group ID | `rocksdict-checkpoint-consumer` |
| `KAFKA_TOPICS` | 訂閱的 topic，逗號分隔 | `CCARDTD_STREAM_FINAL` |
| `KAFKA_AUTO_OFFSET_RESET` | offset 重置策略 | `earliest` |
| `ROCKSDB_PATH` | RocksDB 存放目錄 | `./data/checkpoints` |
| `CHECKPOINT_EVERY_RECORDS` | 每幾筆 flush 一次 | `1000` |
| `CHECKPOINT_EVERY_SECONDS` | 每幾秒 flush 一次 | `5` |
| `COMMIT_KAFKA_OFFSETS` | 是否同步手動 commit Kafka offset | `false` |
| `POLL_TIMEOUT_SECONDS` | Kafka poll 等待時間 | `1.0` |

各 process 可在自己的 `processing.py` 內讀取專屬環境變數。

## Docker Compose

`docker-compose.yaml` 會啟動以下服務：

| 服務 | 說明 |
|------|------|
| `kafka` | KRaft 模式的 Kafka broker，container 內 `kafka:29092`，本機 `localhost:9092` |
| `akhq` | Kafka UI，http://localhost:8080 |
| `consumer` | kafka-consumer container，訂閱 `Click` topic，`CONSUMER_PROCESS=SL0001` |
| `consumer1` | 同上，與 `consumer` 同一個 group，Kafka 自動分配 partition |

啟動：

```powershell
docker compose up --build
```

送一筆測試資料：

```powershell
docker compose exec kafka kafka-console-producer --bootstrap-server kafka:29092 --topic Click
```

輸入一行 JSON 後按 Enter：

```json
{"card_id":"demo-1","amount":123}
```

查看 consumer log：

```powershell
docker compose logs -f consumer
docker compose logs -f consumer1
```

## 多 Process 架構（Plugin 設計）

`common/` 為共用框架，業務邏輯由各 `model_scenarios/` 目錄自行定義。`service/app.py` 在啟動時讀取 `CONSUMER_PROCESS` 環境變數，動態 import 對應的 `processing.py`：

```
CONSUMER_PROCESS=SL0001  →  model_scenarios.SL0001.processing.process_record
CONSUMER_PROCESS=SL0002  →  model_scenarios.SL0002.processing.process_record
```

### 新增一個 process

1. 在 `model_scenarios/` 下建立新目錄，例如 `SL0002/`
2. 新增 `__init__.py` 和 `processing.py`，實作符合 `ProcessorProtocol` 的 `process_record` function
3. k8s Deployment 或 docker-compose 設定 `CONSUMER_PROCESS=SL0002`

### K8s 水平擴展

- 每個 process 對應一個獨立的 k8s Deployment
- 同一個 Deployment 可開多個 replica，Kafka consumer group 自動分配 partition
- 不同 Deployment 使用不同 `KAFKA_GROUP_ID`

### `process_record` 介面

```python
def process_record(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    previous_state: dict[str, Any] | None,
) -> dict[str, Any]:
    ...
```

回傳的 dict 會作為中間狀態與 offset 一起寫入 RocksDB checkpoint。

## Checkpoint 格式

RocksDB key：

```
checkpoint:{topic}:{partition}
```

Value 是 JSON：

```json
{
  "topic": "Click",
  "partition": 0,
  "next_offset": 1235,
  "intermediate_state": {
    "processed_count": 1235
  },
  "updated_at": 1770000000.0
}
```

`next_offset` 使用 Kafka 慣例：處理完成 offset `1234` 之後，checkpoint 記錄下一次要讀取的 offset `1235`。

## Kubernetes 多 pod 提醒

- 所有 pod 使用相同 `KAFKA_GROUP_ID`
- pod 數量超過 topic partition 數不會增加吞吐量；同一個 consumer group 最大平行度是 partition 數
- `ROCKSDB_PATH` 在容器內應指到可寫目錄（建議掛 persistent volume）
- 如果依賴 RocksDB checkpoint 做 restart/rebalance recovery，請搭配 `COMMIT_KAFKA_OFFSETS=true`
