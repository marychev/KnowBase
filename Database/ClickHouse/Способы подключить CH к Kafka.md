# Способы подключить CH к Kafka

```
Инструмент			Гарантии				Куда смотрит					Сложность
ClickPipes			at-least-once			только Cloud, только consume	низкая (managed)
Kafka Connect Sink	exactly-once, Debezium	self-hosted, consume			высокая
Kafka Table Engine	at-least-once			self-hosted, consume + produce	низкая
```
