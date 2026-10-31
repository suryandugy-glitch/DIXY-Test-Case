# DIXY 200-Thread Stress Test

Single-file Java 11+ application (`DixyStressTest.java`, no dependencies) that drives
a heavy, deadlock-free workload so DIXY's agent and Spring Boot backend can be
checked for reliability and false positives.

## Quick start

```bash
export DIXY_AGENT_JAR=/path/to/dixy-agent.jar
export DIXY_AGENT_ARGS=""            # optional agent options, e.g. backend URL (see DIXY docs)
./run.sh                             # 200 threads, 30 s, medium contention
./run.sh --deadlock-test             # + one controlled 2-thread deadlock
```

Equivalent manual command:

```bash
javac -d out DixyStressTest.java
java -javaagent:dixy-agent.jar -cp out DixyStressTest --threads=200 --duration=30
```

## What it does

* Spawns N threads (default 200) performing ReentrantLock (ordered multi-lock), nested
  `synchronized`, `Condition.await`, `Object.wait`, `Semaphore`, and `Thread.sleep`.
* Locks are always taken in ascending index order, so a real deadlock is impossible in normal mode.
* Console dashboard: live RUNNING / BLOCKED / WAITING / TERMINATED counts, a per-thread
  grid (R/B/W/T), ops/s, heap, JVM deadlock check and DIXY alert count.
* Verification: all threads started, executed and terminated; no failed operations; shared
  counters guarded by the locks match per-thread tallies (detects lost updates); the JVM's own
  `ThreadMXBean.findDeadlockedThreads()` reports none; DIXY raised no alerts.
* Writes `dixy-stress-report.md` and `.json`.
* Exit code: `0` PASS, `1` FAIL, `2` INCONCLUSIVE (JVM-side checks ran but the DIXY backend couldn't be queried).

## Options

| Option | Default | Meaning |
|---|---|---|
| `--threads` | 200 | thread count |
| `--duration` | 30 | workload seconds |
| `--contention` | medium | `low`, `medium`, `high`, `extreme` presets |
| `--locks`, `--hot-locks`, `--hot-pct` | preset | fine-tune contention (fewer locks / more hot-lock traffic = more BLOCKED threads) |
| `--hold-micros` | 200 | average lock hold time |
| `--max-locks-per-op` | 3 | locks taken together per operation |
| `--deadlock-test` | off | adds a deliberate lock-order inversion (uses 2 of the N threads) |
| `--detect-timeout` | 30 | seconds to wait for detection in deadlock mode |
| `--dixy-url` | http://localhost:8080 | DIXY backend (`none` disables polling) |
| `--dixy-deadlocks-path` | /api/deadlocks | endpoint listing detected deadlocks |
| `--report` | dixy-stress-report | output basename |
| `--save-baseline` / `--baseline` | - | overhead measurement (below) |

## DIXY backend integration (adjust if needed)

The agent itself sends thread/lock telemetry to the backend; this app does not duplicate that.
To check alerts, the app polls `GET <dixy-url><dixy-deadlocks-path>` and counts the records in the
JSON response (a top-level array, or an object with `totalElements`/`count`/`total`, or the first
array inside it). It compares the count before and after the run, so older alerts are ignored.
If your backend uses a different endpoint, set `--dixy-deadlocks-path`. If it is unreachable the
DIXY checks are marked SKIP and the verdict is INCONCLUSIVE rather than a false PASS.

## Measuring monitoring overhead

```bash
NO_AGENT=1 ./run.sh --save-baseline=base.json --report=baseline-report --dixy-url=none
./run.sh --baseline=base.json
```

Use identical options for both runs. The report shows throughput overhead, process CPU time,
peak heap and GC time. (The dashboard's own sampling is included in both runs.)

## Deadlock test mode

`--deadlock-test` runs N-2 normal workers plus two threads that acquire locks A/B in opposite
order. The test passes when the JVM confirms the deadlock **and** DIXY reports it. Afterwards the
two threads are interrupted (they use `lockInterruptibly`) so every thread still terminates.
