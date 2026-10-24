# Run `etl_pipeline` example

```sh
cd Goland/Concurrent\ programming/examples/etl_pipeline/

go mod init pipeline
# Terminal Responce:
# go: creating new go.mod: module pipeline
# go: to add module requirements and sums:
#         go mod tidy

go test -v
# Terminal Responce:
# === RUN   TestNormalizeWeb
# --- PASS: TestNormalizeWeb (0.00s)
# === RUN   TestNormalizeApp
# --- PASS: TestNormalizeApp (0.00s)
# === RUN   TestMerge_AllEventsArriveAndChannelCloses
# --- PASS: TestMerge_AllEventsArriveAndChannelCloses (0.00s)
# === RUN   TestMerge_CancelStopsAndCloses
# --- PASS: TestMerge_CancelStopsAndCloses (0.00s)
# === RUN   TestBatch_FlushBySize
# --- PASS: TestBatch_FlushBySize (0.00s)
# === RUN   TestBatch_FlushByTimeout
# --- PASS: TestBatch_FlushByTimeout (0.12s)
# === RUN   TestBatch_FlushesRemainderOnClose
# --- PASS: TestBatch_FlushesRemainderOnClose (0.00s)
# === RUN   TestRunPipeline_EndToEnd_NoEventLost
# --- PASS: TestRunPipeline_EndToEnd_NoEventLost (0.00s)
# === RUN   TestRunPipeline_CancelShutsDownCleanly
# --- PASS: TestRunPipeline_CancelShutsDownCleanly (0.04s)
# PASS
# ok      pipeline        0.164s
```

## Что можно сделать лучше?

- Много дублирующегося кода. Стоит изучить дженерики
- 