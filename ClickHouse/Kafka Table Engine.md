# Kafka Table Engine

FastAPI принимает данные от пользователей и пишет в Kafka, а ClickHouse через Kafka Engine их забирает.

Минус (at-least-once) для текущего нагрузочного стенда не критичен — у тебя в Python-стеке всё равно acks=1 на producer'е, exactly-once там и так нет.

Архитектурные плюсы:
- DDL отделён от runtime. lifespan API больше ничего не знает про CH-схему.
- Идемпотентность. IF NOT EXISTS уже стояли в DDL → повторный запуск безопасен.
- Только 1 раз на стек. Не на воркер (UVICORN_WORKERS=5), а ровно один контейнер.
- Гейтинг через depends_on. API не стартует, пока ch-init не вышел с кодом 0.
- Минимум кода. Образ переиспользуется (build: .), `__main__` блок 2 строки.

> End-to-end: `POST → Kafka → CH Kafka Engine → MV → users` 



## Вот как организовать этот процесс

### 1. FastAPI: Отправка в Kafka:

На стороне FastAPI важно не блокировать основной поток. Обычно используют библиотеку aiokafka.
```py
from aiokafka import AIOKafkaProducer
import json

async def send_event(topic, data):
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        enable_idempotence=True  # Защита от дублей при ретраях
    )
    await producer.start()
    try:
        await producer.send_and_wait(topic, json.dumps(data).encode('utf-8'))
    finally:
        await producer.stop()
```


### 2. ClickHouse: Настройка Kafka Engine

В самом ClickHouse архитектура состоит из трех элементов:
- Таблица-движок (Kafka Engine): «Труба», которая только читает топик.
- Целевая таблица (MergeTree / ReplicatedMergeTree): Где данные лежат физически.
- Материализованное представление (Materialized View): «Триггер», который перекладывает данные из трубы в целевую таблицу.

```sql
-- 1. "Труба" (Kafka Engine)
CREATE TABLE queue (
    user_id UInt64,
    event_name String,
    timestamp DateTime
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:9092',
         kafka_topic_list = 'events',
         kafka_group_name = 'ch_consumers',
         kafka_format = 'JSONEachRow';

-- 2. Физическая таблица
CREATE TABLE events_local (
    user_id UInt64,
    event_name String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id);

-- 3. Мост (Materialized View)
CREATE MATERIALIZED VIEW events_mv TO events_local AS
SELECT user_id, event_name, timestamp
FROM queue;
```


### Example Docker Compose
```yaml
version: '3.8'
services:
  zookeeper:
    image: bitnami/zookeeper:latest
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes

  kafka:
    image: bitnami/kafka:latest
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_ANONYMOUS_LOGIN=yes
    depends_on:
      - zookeeper

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"
      - "9000:9000"
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 10s
      timeout: 5s
      retries: 5

  app:
    build: .
    ports:
      - "8000:8000"
    depends_on:
      clickhouse:
        condition: service_healthy
```


### Example Dockerfile
```sh
# Используем официальный Python образ
FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY main.py .

# Запускаем сервер uvicorn
CMD ["uvicorn", "main.py", "--host", "0.0.0.0", "--port", "8000"]
```



## Важный совет по отладке:

Если данные не появляются в ClickHouse:
- Логи MV: Если в Materialized View ошибка (например, несовпадение типов данных), ClickHouse просто «тихо» перестанет забирать данные. Проверить ошибки можно запросом:`SELECT * FROM system.errors WHERE last_error_time > now() - interval 5 minute;`
- Kafka Engine: Помните, что таблица kafka_queue — это «поток». Если вы сделаете из неё SELECT, она прочитает сообщения и сдвинет оффсет. В следующий раз данных там не будет. Всегда проверяйте результат в итоговой таблице events_final.

