# RocksDict Kafka Checkpoint Consumer

這是一個 Python consumer 範例，使用：

- Python `3.14.4`
- `rocksdict==0.3.29`
- `confluent-kafka==2.14.0`

Consumer 會關閉 Kafka `enable.auto.commit`，每筆 Kafka message 處理完成後先暫存在記憶體，並依照「每 N 筆」或「每 X 秒」flush 到 RocksDB。RocksDB checkpoint 內容包含下一次要讀的 offset 和中間狀態。

## 架構重點

RocksDB 在這裡負責：

- Offset 管理：記錄每個 `topic + partition` 已安全處理完成後的 `next_offset`
- Checkpoint：每 N 筆或每 X 秒保存安全點
- 中間狀態：保存 fake processing function 回傳的 state

多個 pod 讀同一個 topic 時，請使用相同 `group.id`。Kafka 會把同一個 consumer group 內的 partition 分配給不同 pod；同一個 partition 同時間只會由其中一個 pod 消費。

重要限制：如果 RocksDB 放在 pod 本機 ephemeral disk，partition rebalance 到另一個 pod 時，那個 pod 可能沒有該 partition 的 RocksDB checkpoint。正式環境要避免這個問題，通常會選其中一種：

- 每個 pod 掛 persistent volume，並設計 partition 與 state 的搬移策略
- 保留 RocksDB 作為本機 state store，同時設定 `COMMIT_KAFKA_OFFSETS=true`，在 RocksDB checkpoint flush 成功後才手動 commit Kafka offset，讓 consumer group rebalance 有共同 offset 可用
- 使用外部共享 state store 保存 checkpoint

此範例預設 `COMMIT_KAFKA_OFFSETS=false`，符合「不依賴 Kafka auto.commit」。要支援多 pod rebalance 更穩定，建議在部署時改為 `true`；這仍然是手動 commit，不是 auto commit。

## 安裝

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

## 設定

複製 `.env.example` 後設定環境變數，或直接在 shell 設定：

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:KAFKA_GROUP_ID="rocksdict-checkpoint-consumer"
$env:KAFKA_TOPICS="orders"
$env:ROCKSDB_PATH="./data/checkpoints"
$env:CHECKPOINT_EVERY_RECORDS="1000"
$env:CHECKPOINT_EVERY_SECONDS="5"
$env:COMMIT_KAFKA_OFFSETS="false"
```

## 執行

```powershell
python -m checkpoint_consumer.app
```

或安裝成 package 後執行：

```powershell
checkpoint-consumer
```

## Docker Compose

這個 repo 也包含 `docker-compose.yaml`，會啟動：

- Kafka：container 內使用 `kafka:29092`，本機使用 `localhost:9092`
- Kafka init job：先建立 `orders` topic，避免 consumer 比 topic 更早啟動
- AKHQ：http://localhost:8080
- Python consumer：使用同一份 Dockerfile build，讀取 `orders` topic

啟動：

```powershell
docker compose up --build
```

送一筆測試資料：

```powershell
docker compose exec kafka kafka-console-producer --bootstrap-server kafka:29092 --topic orders
```

輸入一行 JSON 後按 Enter：

```json
{"order_id":"demo-1","amount":123}
```

查看 consumer log：

```powershell
docker compose logs -f consumer
```

## 處理資料的位置

目前資料處理在 `src/checkpoint_consumer/processing.py` 的 `fake_process_record()`。替換這個 function 即可接上真正的 business logic。

這個 function 回傳的 dict 會被當成 partition 的中間狀態，和 offset 一起寫入 RocksDB。

## Checkpoint 格式

RocksDB key：

```text
checkpoint:{topic}:{partition}
```

Value 是 JSON：

```json
{
  "topic": "orders",
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

範例部署時要確保：

- 所有 pod 使用相同 `KAFKA_GROUP_ID`
- pod 數量不要期待超過 topic partition 數還能提升吞吐量；同一個 consumer group 的最大平行度是 partition 數
- `ROCKSDB_PATH` 在容器內應該指到可寫目錄
- 如果依賴 RocksDB checkpoint 做 restart/rebalance recovery，請使用 persistent volume 或搭配 `COMMIT_KAFKA_OFFSETS=true`
