# agentscope-streaming-repro

Minimal repro for a streaming-pipeline latency inflation observed with `ReActAgent` (AgentScope Java `2.0.4`) at high concurrency.

A stub OpenAI-compatible SSE server (`StubLlmServer`) returns a deterministic tool-calling chain. `ReproMain` drives `N` concurrent `ReActAgent` instances against it through `streamEvents` and reports per-chain latency and effective tokens/sec.

## What it demonstrates

* Each chain is a deterministic 10-model-call / 9-tool-execution loop: `get_weather` x5 -> `get_time` x4, then a final answer.
* The stub server emits tokens at a fixed nominal rate (e.g. 50 tok/s at `tokensPerCall=300, tokenIntervalMs=20`).
* At low concurrency (chains=5) the effective rate matches the nominal rate (~49 tok/s).
* At high concurrency (chains=500, 4 vCPU) the mean chain time grows from ~61s to ~80s and effective rate falls to ~36-38 tok/s, even though the stub is stateless and each chain's token schedule is unchanged. The effect is present for both `metronome` (one token per flush) and `batch100` (100 tokens per flush) delivery, so it is not an artefact of per-token SSE framing.

## Files

| File | Purpose |
|---|---|
| `pom.xml` | Maven build (AgentScope Java 2.0.4, Java 21, OkHttp, Jackson, JUnit) |
| `src/main/java/repro/StubLlmServer.java` | Deterministic stub SSE server (JDK `HttpServer`) |
| `src/main/java/repro/ReproMain.java` | Driver: N concurrent `ReActAgent` chains via `streamEvents` |
| `src/main/java/repro/ReproTools.java` | `@Tool`-annotated `get_weather` / `get_time` tools |
| `smoke-test.log` | Smoke-test output (see below) |

## Run

```bash
# Build
mvn -q -DskipTests package

# Smoke: 5 chains, small params (fast)
mvn -q exec:java -Dexec.args="--chains 5 --tokens 30 --interval 2 --delivery metronome"

# Standard params, 5 chains (~61 s)
mvn -q exec:java -Dexec.args="--chains 5 --tokens 300 --interval 20 --delivery metronome"

# Standard params, 500 chains (~86-88 s wall, phenomenon check)
mvn -q exec:java -Dexec.args="--chains 500 --tokens 300 --interval 20 --delivery batch100"
```

Arguments (all optional, defaults shown):

```text
--chains 5            number of concurrent ReActAgent chains
--tokens 300          tokens emitted per model call by the stub
--interval 20         ms between stub token emissions (metronome) or per 100-token batch
--delivery metronome  metronome | batch100
--port 0              stub port (0 = ephemeral)
```

## Smoke-test results

`smoke-test.log` was produced on a 4-vCPU machine with the commands above (plus the batch100 variants). Summary:

| Smoke | Chains | Delivery | Wall | Mean chain | Eff. tok/s per chain | Tool invocations |
|---|---|---|---|---|---|---|
| 1 | 5 | metronome, small | 3.20s | 3.12s | 96.07 (nominal 500) | 45 / 45 |
| 2 | 5 | batch100, small | 3.14s | 3.02s | 99.30 (nominal 500) | 45 / 45 |
| 3 | 5 | metronome, standard | 61.29s | 61.18s | 49.04 (nominal 50) | 45 / 45 |
| 4 | 5 | batch100, standard | 61.20s | 61.08s | 49.12 (nominal 50) | 45 / 45 |
| 5 | 500 | batch100, standard | 88.12s | 83.27s | 36.03 (nominal 50) | 4500 / 4500 |
| 6 | 500 | metronome, standard | 85.96s | 79.62s | 37.68 (nominal 50) | 4500 / 4500 |

In every run the count assertion passed: every chain showed exactly 10 model calls and 9 tool executions, so the slowdown is not caused by extra model calls, retries, or failed chains.

## Notes / limitations

* The stub server is intentionally trivial (no auth, no real model, fixed tool-call sequence driven by a `state` field in the request body). It is not a general OpenAI mock.
* Token counts are derived from the stub's fixed schedule, not from a real tokenizer.
* The repro uses `ReActAgent` defaults except for the model/tools wiring shown in `ReproMain`; no custom executor or thread-pool tuning is applied, which is part of the question for the maintainers (see the issue): is the inflation expected from a shared internal pipeline, and is there a supported way to scale it?
