import java.io.IOException;
import java.lang.management.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DIXY 200-thread stress test.
 *
 * Normal mode  : N worker threads hammer ordered locks, monitors, conditions,
 *                wait/notify and semaphores. Lock ordering is globally
 *                consistent, so NO deadlock can occur. DIXY must stay silent.
 * Deadlock mode: --deadlock-test adds one deliberate 2-thread lock-order
 *                inversion. DIXY must report it.
 *
 * Attach the agent externally:  java -javaagent:dixy-agent.jar -jar / -cp ...
 */
public class DixyStressTest {

    // ------------------------------------------------------------------ config
    static final class Config {
        int threads = 200, durationSec = 30, locks = 50, hotLocks = 4, hotPct = 30;
        int holdMicros = 200, refreshMs = 1000, graceSec = 15, detectTimeoutSec = 30, maxLocksPerOp = 3;
        String contention = "medium";
        boolean deadlockTest = false;
        boolean ansi = System.console() != null;
        String dixyUrl = "http://localhost:8080";
        String dixyPath = "/api/deadlocks";
        String report = "dixy-stress-report";
        String baselineFile = null, saveBaseline = null;

        static Config parse(String[] args) {
            Map<String, String> m = new HashMap<>();
            for (String a : args) {
                if (!a.startsWith("--")) throw new IllegalArgumentException("Bad argument: " + a);
                int eq = a.indexOf('=');
                if (eq < 0) m.put(a.substring(2), "true");
                else m.put(a.substring(2, eq), a.substring(eq + 1));
            }
            Config c = new Config();
            if (m.containsKey("help")) { usage(); System.exit(0); }
            c.threads = i(m, "threads", c.threads);
            c.durationSec = i(m, "duration", c.durationSec);
            c.contention = m.getOrDefault("contention", c.contention).toLowerCase();
            switch (c.contention) {
                case "low":     c.locks = Math.max(200, c.threads); c.hotLocks = 1; c.hotPct = 0;  break;
                case "medium":  c.locks = 50; c.hotLocks = 4; c.hotPct = 30; break;
                case "high":    c.locks = 10; c.hotLocks = 2; c.hotPct = 80; break;
                case "extreme": c.locks = 4;  c.hotLocks = 1; c.hotPct = 90; break;
                default: throw new IllegalArgumentException("--contention must be low|medium|high|extreme");
            }
            c.locks = i(m, "locks", c.locks);
            c.hotLocks = i(m, "hot-locks", c.hotLocks);
            c.hotPct = i(m, "hot-pct", c.hotPct);
            c.holdMicros = i(m, "hold-micros", c.holdMicros);
            c.refreshMs = i(m, "refresh-ms", c.refreshMs);
            c.graceSec = i(m, "grace", c.graceSec);
            c.detectTimeoutSec = i(m, "detect-timeout", c.detectTimeoutSec);
            c.maxLocksPerOp = i(m, "max-locks-per-op", c.maxLocksPerOp);
            c.deadlockTest = m.containsKey("deadlock-test");
            if (m.containsKey("ansi")) c.ansi = true;
            if (m.containsKey("no-ansi")) c.ansi = false;
            c.dixyUrl = m.getOrDefault("dixy-url", c.dixyUrl);
            c.dixyPath = m.getOrDefault("dixy-deadlocks-path", c.dixyPath);
            c.report = m.getOrDefault("report", c.report);
            c.baselineFile = m.get("baseline");
            c.saveBaseline = m.get("save-baseline");
            c.hotLocks = Math.max(1, Math.min(c.hotLocks, c.locks));
            c.maxLocksPerOp = Math.max(1, Math.min(c.maxLocksPerOp, c.locks));
            if (c.threads < (c.deadlockTest ? 3 : 1)) throw new IllegalArgumentException("--threads too small");
            if (c.locks < 1) throw new IllegalArgumentException("--locks must be >= 1");
            return c;
        }

        static int i(Map<String, String> m, String k, int d) {
            return m.containsKey(k) ? Integer.parseInt(m.get(k)) : d;
        }

        String fingerprint() {
            return "threads=" + threads + ";duration=" + durationSec + ";locks=" + locks + ";hotLocks=" + hotLocks
                    + ";hotPct=" + hotPct + ";holdMicros=" + holdMicros + ";maxLocksPerOp=" + maxLocksPerOp;
        }
    }

    static void usage() {
        System.out.println(String.join("\n",
            "DIXY stress test - options (all --key=value):",
            "  --threads=200            worker thread count",
            "  --duration=30            workload duration in seconds",
            "  --contention=medium      low | medium | high | extreme (presets for locks/hot-locks/hot-pct)",
            "  --locks=N --hot-locks=N --hot-pct=0..100   override contention preset",
            "  --hold-micros=200        average time each lock is held (busy spin)",
            "  --max-locks-per-op=3     max locks taken together in one operation",
            "  --deadlock-test          ALSO create one controlled 2-thread deadlock (uses 2 of the threads)",
            "  --detect-timeout=30      seconds to wait for detection in deadlock mode",
            "  --dixy-url=http://localhost:8080   DIXY backend base URL ('none' to disable polling)",
            "  --dixy-deadlocks-path=/api/deadlocks   endpoint returning detected deadlocks (JSON)",
            "  --report=dixy-stress-report   output basename (.md and .json are written)",
            "  --save-baseline=file.json     save throughput for a run WITHOUT the agent",
            "  --baseline=file.json          compare throughput against a saved baseline (overhead)",
            "  --refresh-ms=1000  --grace=15  --ansi | --no-ansi"));
    }

    // ------------------------------------------------------------------- state
    final Config cfg;
    final ReentrantLock[] locks;
    final Condition[] conds;
    final Object[] monitors;
    final long[] rlCounters, monCounters;       // guarded by locks[i] / monitors[i]
    final long[] rlInc, monInc;                 // per-thread tallies, written only by the owner
    final Semaphore sem;
    final CountDownLatch startGate = new CountDownLatch(1);
    final AtomicLongArray threadOps;
    final LongAdder okOps = new LongAdder(), failedOps = new LongAdder();
    final LongAdder[] opByType;
    static final String[] OP_NAMES = {"ReentrantLock (ordered multi-lock)", "synchronized (nested)",
            "Condition.await", "Object.wait", "Semaphore", "Thread.sleep"};
    final AtomicInteger startedCount = new AtomicInteger();
    final AtomicLong lastWorkerFinish = new AtomicLong();
    final Queue<String> errors = new ConcurrentLinkedQueue<>();
    final Thread[] threads;
    volatile boolean stop = false;
    final int normalWorkers;

    // deadlock scenario
    final ReentrantLock dlA = new ReentrantLock(), dlB = new ReentrantLock();
    final CyclicBarrier dlBarrier = new CyclicBarrier(2);

    // monitoring
    final ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
    final Set<String> localDeadlockedNames = ConcurrentHashMap.newKeySet();
    volatile long localDeadlockFirstSeenNanos = 0;
    final DixyClient dixy;
    volatile long dixyBaseline = -1, dixyCurrent = -1;
    volatile boolean dixyReachable = false;
    final int[] peak = new int[4];            // R B W T
    final List<double[]> timeline = new ArrayList<>(); // t, R, B, W, T, ops/s
    long peakHeapBytes = 0;
    final AtomicInteger interruptedForCleanup = new AtomicInteger();

    DixyStressTest(Config c) {
        cfg = c;
        locks = new ReentrantLock[c.locks];
        conds = new Condition[c.locks];
        monitors = new Object[c.locks];
        for (int i = 0; i < c.locks; i++) {
            locks[i] = new ReentrantLock();
            conds[i] = locks[i].newCondition();
            monitors[i] = new Object();
        }
        rlCounters = new long[c.locks];
        monCounters = new long[c.locks];
        rlInc = new long[c.threads];
        monInc = new long[c.threads];
        sem = new Semaphore(Math.max(1, c.threads / 4));
        threadOps = new AtomicLongArray(c.threads);
        opByType = new LongAdder[OP_NAMES.length];
        for (int i = 0; i < opByType.length; i++) opByType[i] = new LongAdder();
        threads = new Thread[c.threads];
        normalWorkers = c.deadlockTest ? c.threads - 2 : c.threads;
        dixy = new DixyClient(c.dixyUrl, c.dixyPath);
    }

    // ---------------------------------------------------------------- workload
    long deadlineNanos;

    void worker(int id) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        startedCount.incrementAndGet();
        try {
            startGate.await();
            while (!stop && System.nanoTime() < deadlineNanos) {
                int op = pickOp(r);
                try {
                    switch (op) {
                        case 0: opLock(id, r); break;
                        case 1: { int[] a = pickLocks(r, 1 + r.nextInt(cfg.maxLocksPerOp)); syncNested(a, 0, id, r); break; }
                        case 2: opCond(id, r); break;
                        case 3: opWait(id, r); break;
                        case 4: opSem(r); break;
                        default: Thread.sleep(1 + r.nextInt(3));
                    }
                    okOps.increment();
                    opByType[op].increment();
                    threadOps.incrementAndGet(id);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Throwable t) {
                    failedOps.increment();
                    if (errors.size() < 10) errors.add("worker-" + id + " op" + op + ": " + t);
                }
            }
        } catch (InterruptedException ignored) {
        } finally {
            lastWorkerFinish.accumulateAndGet(System.nanoTime(), Math::max);
        }
    }

    static int pickOp(ThreadLocalRandom r) {
        int x = r.nextInt(100);
        if (x < 40) return 0;
        if (x < 65) return 1;
        if (x < 75) return 2;
        if (x < 83) return 3;
        if (x < 93) return 4;
        return 5;
    }

    /** Distinct lock indexes in ASCENDING order => consistent global lock order => no deadlock. */
    int[] pickLocks(ThreadLocalRandom r, int k) {
        int[] a = new int[k];
        int n = 0;
        while (n < k) {
            int c = (cfg.hotPct > 0 && r.nextInt(100) < cfg.hotPct) ? r.nextInt(cfg.hotLocks) : r.nextInt(cfg.locks);
            boolean dup = false;
            for (int j = 0; j < n; j++) if (a[j] == c) { dup = true; break; }
            if (!dup) a[n++] = c;
        }
        Arrays.sort(a);
        return a;
    }

    void hold(ThreadLocalRandom r) {
        if (cfg.holdMicros <= 0) return;
        long end = System.nanoTime() + (long) (cfg.holdMicros * 1000L * (0.5 + r.nextDouble()));
        while (System.nanoTime() < end) Thread.onSpinWait();
    }

    void opLock(int id, ThreadLocalRandom r) {
        int[] a = pickLocks(r, 1 + r.nextInt(cfg.maxLocksPerOp));
        int n = 0;
        try {
            for (int i : a) { locks[i].lock(); n++; }
            for (int i : a) rlCounters[i]++;
            rlInc[id] += a.length;
            hold(r);
            if (r.nextInt(8) == 0) for (int i : a) conds[i].signalAll();
        } finally {
            for (int j = n - 1; j >= 0; j--) locks[a[j]].unlock();
        }
    }

    void syncNested(int[] a, int pos, int id, ThreadLocalRandom r) {
        if (pos == a.length) {
            for (int i : a) monCounters[i]++;
            monInc[id] += a.length;
            hold(r);
            return;
        }
        synchronized (monitors[a[pos]]) {
            syncNested(a, pos + 1, id, r);
            if (r.nextInt(8) == 0) monitors[a[pos]].notifyAll();
        }
    }

    void opCond(int id, ThreadLocalRandom r) throws InterruptedException {
        int i = pickLocks(r, 1)[0];
        locks[i].lock();
        try {
            conds[i].await(1 + r.nextInt(3), TimeUnit.MILLISECONDS);
            rlCounters[i]++;
            rlInc[id]++;
        } finally {
            locks[i].unlock();
        }
    }

    void opWait(int id, ThreadLocalRandom r) throws InterruptedException {
        int i = pickLocks(r, 1)[0];
        synchronized (monitors[i]) {
            monitors[i].wait(1 + r.nextInt(3));
            monCounters[i]++;
            monInc[id]++;
        }
    }

    void opSem(ThreadLocalRandom r) throws InterruptedException {
        sem.acquire();
        try { hold(r); } finally { sem.release(); }
    }

    /** Controlled lock-order inversion. role 0: A then B, role 1: B then A. Uses interruptible locks so we can clean up. */
    void deadlockThread(int id, int role) {
        startedCount.incrementAndGet();
        ReentrantLock first = role == 0 ? dlA : dlB, second = role == 0 ? dlB : dlA;
        boolean h1 = false, h2 = false;
        try {
            startGate.await();
            first.lockInterruptibly(); h1 = true;
            threadOps.incrementAndGet(id);
            dlBarrier.await();                    // both threads now hold their first lock
            second.lockInterruptibly(); h2 = true; // never succeeds
        } catch (InterruptedException | BrokenBarrierException e) {
            interruptedForCleanup.incrementAndGet();
        } finally {
            if (h2) second.unlock();
            if (h1) first.unlock();
        }
    }

    // -------------------------------------------------------------------- main
    int exitCode;

    int run() throws Exception {
        long wallStart = System.currentTimeMillis();
        boolean agent = ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .anyMatch(s -> s.startsWith("-javaagent") && s.toLowerCase().contains("dixy"));
        System.out.printf("DIXY stress test | threads=%d duration=%ds contention=%s locks=%d hot=%d@%d%% mode=%s%n",
                cfg.threads, cfg.durationSec, cfg.contention, cfg.locks, cfg.hotLocks, cfg.hotPct,
                cfg.deadlockTest ? "DEADLOCK-TEST" : "NORMAL");
        System.out.println("DIXY agent attached: " + (agent ? "yes" : "NO (-javaagent:dixy-agent.jar not found in JVM args)"));

        if (!cfg.dixyUrl.equalsIgnoreCase("none")) {
            dixyBaseline = dixy.fetchCount();
            dixyCurrent = dixyBaseline;
            dixyReachable = dixyBaseline >= 0;
            System.out.println("DIXY backend (" + cfg.dixyUrl + cfg.dixyPath + "): "
                    + (dixyReachable ? "reachable, existing alerts=" + dixyBaseline : "NOT reachable / unparsable - DIXY checks will be skipped"));
        }

        for (int i = 0; i < cfg.threads; i++) {
            final int id = i;
            Thread t;
            if (cfg.deadlockTest && i >= normalWorkers) {
                final int role = i - normalWorkers;
                t = new Thread(() -> deadlockThread(id, role), "dixy-deadlock-" + (role + 1));
            } else {
                t = new Thread(() -> worker(id), String.format("dixy-stress-worker-%03d", id));
            }
            threads[i] = t;
        }

        com.sun.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean
                ? (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean() : null;
        long cpu0 = os != null ? os.getProcessCpuTime() : -1;
        long gcCount0 = 0, gcTime0 = 0;
        for (GarbageCollectorMXBean g : ManagementFactory.getGarbageCollectorMXBeans()) { gcCount0 += g.getCollectionCount(); gcTime0 += g.getCollectionTime(); }

        Thread poller = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && dixyReachable) {
                long c = dixy.fetchCount();
                if (c >= 0) dixyCurrent = c;
                try { Thread.sleep(2000); } catch (InterruptedException e) { return; }
            }
        }, "dixy-stress-poller");
        poller.setDaemon(true);
        poller.start();

        long start = System.nanoTime();
        deadlineNanos = start + cfg.durationSec * 1_000_000_000L;
        long hardDeadline = deadlineNanos + (cfg.graceSec + (cfg.deadlockTest ? cfg.detectTimeoutSec : 0)) * 1_000_000_000L;
        for (Thread t : threads) t.start();
        startGate.countDown();

        boolean hung = false, cleanedUp = false;
        long lastOps = 0, lastTick = start;
        long deadlockResolveBy = Long.MAX_VALUE;
        if (cfg.ansi) System.out.print("\033[2J");

        while (true) {
            Thread.sleep(cfg.refreshMs);
            long now = System.nanoTime();

            // thread states
            int[] cnt = new int[5]; // R B W T N
            StringBuilder grid = new StringBuilder();
            for (int i = 0; i < threads.length; i++) {
                char c = code(threads[i].getState());
                grid.append(c);
                switch (c) { case 'R': cnt[0]++; break; case 'B': cnt[1]++; break; case 'W': cnt[2]++; break; case 'T': cnt[3]++; break; default: cnt[4]++; }
                if ((i + 1) % 50 == 0) grid.append('\n');
            }
            for (int i = 0; i < 4; i++) peak[i] = Math.max(peak[i], cnt[i]);

            // local ground-truth deadlock check (JVM ThreadMXBean)
            long[] dl = tmx.findDeadlockedThreads();
            if (dl != null) {
                for (ThreadInfo ti : tmx.getThreadInfo(dl)) if (ti != null) localDeadlockedNames.add(ti.getThreadName());
                if (localDeadlockFirstSeenNanos == 0) {
                    localDeadlockFirstSeenNanos = now;
                    deadlockResolveBy = now + cfg.detectTimeoutSec * 1_000_000_000L;
                }
            }

            long ok = okOps.sum();
            double opsPerSec = (ok - lastOps) / Math.max(1e-9, (now - lastTick) / 1e9);
            lastOps = ok; lastTick = now;
            long heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            peakHeapBytes = Math.max(peakHeapBytes, heap);
            timeline.add(new double[]{(now - start) / 1e9, cnt[0], cnt[1], cnt[2], cnt[3], opsPerSec});

            render(now - start, cnt, grid.toString(), ok, opsPerSec, heap);

            // deadlock-mode cleanup: after DIXY alerted (or detect timeout), break the deadlock
            if (cfg.deadlockTest && !cleanedUp && localDeadlockFirstSeenNanos != 0
                    && (dixyAlerts() > 0 || now >= deadlockResolveBy)) {
                for (int i = normalWorkers; i < threads.length; i++) threads[i].interrupt();
                cleanedUp = true;
            }
            if (cfg.deadlockTest && !cleanedUp && localDeadlockFirstSeenNanos == 0
                    && now > deadlineNanos + cfg.detectTimeoutSec * 1_000_000_000L) {
                for (int i = normalWorkers; i < threads.length; i++) threads[i].interrupt();
                cleanedUp = true;
            }

            if (cnt[3] == threads.length) break;
            if (now > hardDeadline) {
                hung = true;
                stop = true;
                for (Thread t : threads) if (t.isAlive()) t.interrupt();
                for (Thread t : threads) t.join(2000);
                break;
            }
        }
        for (Thread t : threads) t.join(2000);
        if (dixyReachable) { Thread.sleep(1500); long c = dixy.fetchCount(); if (c >= 0) dixyCurrent = c; }
        poller.interrupt();
        long end = System.nanoTime();

        long cpu1 = os != null ? os.getProcessCpuTime() : -1;
        long gcCount1 = 0, gcTime1 = 0;
        for (GarbageCollectorMXBean g : ManagementFactory.getGarbageCollectorMXBeans()) { gcCount1 += g.getCollectionCount(); gcTime1 += g.getCollectionTime(); }

        // ---------------------------------------------------------------- results
        double wallSec = (end - start) / 1e9;
        long workEnd = lastWorkerFinish.get() == 0 ? end : lastWorkerFinish.get();
        double workSec = Math.max(1e-9, (workEnd - start) / 1e9);
        long ok = okOps.sum(), failed = failedOps.sum();
        double opsPerSec = ok / workSec;
        int terminated = 0, executed = 0;
        for (int i = 0; i < threads.length; i++) {
            if (threads[i].getState() == Thread.State.TERMINATED) terminated++;
            if (threadOps.get(i) > 0) executed++;
        }
        long rlExpected = 0, monExpected = 0, rlActual = 0, monActual = 0;
        for (long v : rlInc) rlExpected += v;
        for (long v : monInc) monExpected += v;
        for (long v : rlCounters) rlActual += v;
        for (long v : monCounters) monActual += v;
        boolean integrity = rlExpected == rlActual && monExpected == monActual;
        long dixyAlerts = Math.max(0, dixyAlerts());
        double cpuSec = (cpu0 >= 0 && cpu1 >= 0) ? (cpu1 - cpu0) / 1e9 : -1;
        double cpuPct = cpuSec >= 0 ? cpuSec / (wallSec * Runtime.getRuntime().availableProcessors()) * 100 : -1;

        Double baselineOps = null;
        String baselineNote = "no baseline supplied (run once without the agent with --save-baseline=base.json, then pass --baseline=base.json)";
        if (cfg.baselineFile != null) {
            try {
                String bj = Files.readString(Path.of(cfg.baselineFile));
                Matcher m = Pattern.compile("\"opsPerSec\"\\s*:\\s*([0-9.]+)").matcher(bj);
                Matcher f = Pattern.compile("\"config\"\\s*:\\s*\"([^\"]*)\"").matcher(bj);
                if (m.find()) {
                    baselineOps = Double.parseDouble(m.group(1));
                    baselineNote = "baseline " + String.format("%.0f", baselineOps) + " ops/s from " + cfg.baselineFile;
                    if (f.find() && !f.group(1).equals(cfg.fingerprint()))
                        baselineNote += " (WARNING: baseline config differs: " + f.group(1) + ")";
                }
            } catch (IOException e) { baselineNote = "could not read baseline: " + e.getMessage(); }
        }
        Double overheadPct = baselineOps != null && baselineOps > 0 ? (baselineOps - opsPerSec) / baselineOps * 100 : null;

        // checks
        List<String[]> checks = new ArrayList<>(); // name, status, detail
        boolean dixyKnown = dixyReachable;
        if (!cfg.deadlockTest) {
            checks.add(chk("All " + cfg.threads + " threads started", startedCount.get() == cfg.threads, startedCount.get() + "/" + cfg.threads));
            checks.add(chk("All threads executed work", executed == cfg.threads, executed + "/" + cfg.threads));
            checks.add(chk("All threads terminated", terminated == cfg.threads && !hung, terminated + "/" + cfg.threads + (hung ? " (hang, forced interrupt)" : "")));
            checks.add(chk("No failed operations", failed == 0, failed + " failed"));
            checks.add(chk("Shared-state integrity (no lost updates)", integrity,
                    "locks " + rlActual + "/" + rlExpected + ", monitors " + monActual + "/" + monExpected));
            checks.add(chk("No real deadlock (JVM ThreadMXBean ground truth)", localDeadlockedNames.isEmpty(), localDeadlockedNames.size() + " threads"));
            checks.add(dixyKnown ? chk("No false-positive DIXY alert", dixyAlerts == 0, dixyAlerts + " alerts during run")
                                 : new String[]{"No false-positive DIXY alert", "SKIP", "DIXY backend unreachable or response unparsable"});
        } else {
            checks.add(chk("All " + cfg.threads + " threads started", startedCount.get() == cfg.threads, startedCount.get() + "/" + cfg.threads));
            checks.add(chk("Controlled deadlock was created (JVM ground truth)", localDeadlockedNames.size() == 2, localDeadlockedNames.toString()));
            checks.add(dixyKnown ? chk("DIXY detected the deadlock", dixyAlerts > 0, dixyAlerts + " alerts during run")
                                 : new String[]{"DIXY detected the deadlock", "SKIP", "DIXY backend unreachable or response unparsable"});
            checks.add(chk("All threads terminated after cleanup", terminated == cfg.threads && !hung, terminated + "/" + cfg.threads));
            checks.add(chk("Normal workers: no failed operations", failed == 0, failed + " failed"));
            checks.add(chk("Normal workers: shared-state integrity", integrity,
                    "locks " + rlActual + "/" + rlExpected + ", monitors " + monActual + "/" + monExpected));
        }
        boolean anyFail = checks.stream().anyMatch(c -> c[1].equals("FAIL"));
        boolean anySkip = checks.stream().anyMatch(c -> c[1].equals("SKIP"));
        String verdict = anyFail ? "FAIL" : anySkip ? "INCONCLUSIVE" : "PASS";
        exitCode = anyFail ? 1 : anySkip ? 2 : 0;

        // ----------------------------------------------------------------- output
        String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        StringBuilder md = new StringBuilder();
        md.append("# DIXY Stress Test Report\n\n");
        md.append("- Generated: ").append(stamp).append("\n");
        md.append("- Mode: **").append(cfg.deadlockTest ? "Controlled deadlock test" : "Normal (deadlock-free) stress").append("**\n");
        md.append("- Verdict: **").append(verdict).append("**\n");
        md.append("- DIXY agent attached: ").append(agent ? "yes" : "no").append("\n");
        md.append("- JVM: ").append(System.getProperty("java.version")).append(", ").append(Runtime.getRuntime().availableProcessors()).append(" CPUs\n\n");
        md.append("## Configuration\n\n");
        md.append(String.format("| Setting | Value |\n|---|---|\n| Threads | %d |\n| Duration | %d s |\n| Contention | %s (locks=%d, hot locks=%d, hot probability=%d%%) |\n| Lock hold time | ~%d us |\n| Max locks per operation | %d |\n| DIXY backend | %s%s |\n\n",
                cfg.threads, cfg.durationSec, cfg.contention, cfg.locks, cfg.hotLocks, cfg.hotPct, cfg.holdMicros, cfg.maxLocksPerOp,
                cfg.dixyUrl, cfg.dixyUrl.equalsIgnoreCase("none") ? "" : cfg.dixyPath));
        md.append("## Results\n\n| Metric | Value |\n|---|---|\n");
        md.append(String.format("| Workload time | %.2f s |\n| Total wall time (incl. cleanup) | %.2f s |\n", workSec, wallSec));
        md.append(String.format("| Successful operations | %,d |\n| Failed operations | %,d |\n| Throughput | %,.0f ops/s |\n", ok, failed, opsPerSec));
        md.append(String.format("| Threads started / executed / terminated | %d / %d / %d (of %d) |\n", startedCount.get(), executed, terminated, cfg.threads));
        md.append(String.format("| Peak RUNNING / BLOCKED / WAITING / TERMINATED | %d / %d / %d / %d |\n", peak[0], peak[1], peak[2], peak[3]));
        md.append(String.format("| Deadlocks detected by JVM (ground truth) | %d threads %s |\n", localDeadlockedNames.size(), localDeadlockedNames.isEmpty() ? "" : localDeadlockedNames));
        md.append(String.format("| Deadlock alerts raised by DIXY during run | %s |\n", dixyKnown ? String.valueOf(dixyAlerts) : "n/a (backend not reachable)"));
        if (cfg.deadlockTest && localDeadlockFirstSeenNanos != 0)
            md.append(String.format("| Time until deadlock observed locally | %.1f s |\n", (localDeadlockFirstSeenNanos - start) / 1e9));
        md.append("\n## Operation Breakdown\n\n| Operation | Count |\n|---|---|\n");
        for (int i = 0; i < OP_NAMES.length; i++) md.append(String.format("| %s | %,d |\n", OP_NAMES[i], opByType[i].sum()));
        md.append("\n## Monitoring Overhead\n\n");
        md.append(String.format("- Throughput: %,.0f ops/s\n", opsPerSec));
        md.append("- Baseline: ").append(baselineNote).append("\n");
        if (overheadPct != null) md.append(String.format("- **Throughput overhead vs baseline: %.1f%%** (positive = slower with DIXY)\n", overheadPct));
        if (cpuSec >= 0) md.append(String.format("- Process CPU time: %.1f s (%.1f%% of all cores)\n", cpuSec, cpuPct));
        md.append(String.format("- Peak heap used: %.0f MB; GC: %d collections, %d ms\n", peakHeapBytes / 1048576.0, gcCount1 - gcCount0, gcTime1 - gcTime0));
        md.append("- Note: this dashboard samples thread state once per refresh and is included in the measured cost.\n");
        md.append("\n## Verification Checks\n\n| Check | Result | Detail |\n|---|---|---|\n");
        for (String[] c : checks) md.append("| ").append(c[0]).append(" | ").append(c[1]).append(" | ").append(c[2]).append(" |\n");
        if (!errors.isEmpty()) { md.append("\n## Sample Errors\n\n"); for (String e : errors) md.append("- ").append(e).append("\n"); }
        md.append("\n## Thread-State Timeline (sampled)\n\n| t (s) | RUNNING | BLOCKED | WAITING | TERMINATED | ops/s |\n|---|---|---|---|---|---|\n");
        int step = Math.max(1, timeline.size() / 15);
        for (int i = 0; i < timeline.size(); i += step) {
            double[] s = timeline.get(i);
            md.append(String.format("| %.0f | %d | %d | %d | %d | %,.0f |\n", s[0], (int) s[1], (int) s[2], (int) s[3], (int) s[4], s[5]));
        }
        Files.writeString(Path.of(cfg.report + ".md"), md.toString(), StandardCharsets.UTF_8);

        StringBuilder js = new StringBuilder("{\n");
        js.append("  \"config\": \"").append(cfg.fingerprint()).append("\",\n");
        js.append("  \"mode\": \"").append(cfg.deadlockTest ? "deadlock-test" : "normal").append("\",\n");
        js.append("  \"verdict\": \"").append(verdict).append("\",\n");
        js.append("  \"agentAttached\": ").append(agent).append(",\n");
        js.append(String.format(Locale.ROOT, "  \"workloadSeconds\": %.3f,\n  \"successfulOps\": %d,\n  \"failedOps\": %d,\n  \"opsPerSec\": %.1f,\n", workSec, ok, failed, opsPerSec));
        js.append(String.format("  \"threads\": {\"configured\": %d, \"started\": %d, \"executed\": %d, \"terminated\": %d},\n", cfg.threads, startedCount.get(), executed, terminated));
        js.append(String.format("  \"peakStates\": {\"running\": %d, \"blocked\": %d, \"waiting\": %d, \"terminated\": %d},\n", peak[0], peak[1], peak[2], peak[3]));
        js.append(String.format("  \"jvmDeadlockedThreads\": %d,\n  \"dixyAlerts\": %s,\n", localDeadlockedNames.size(), dixyKnown ? String.valueOf(dixyAlerts) : "null"));
        js.append(String.format(Locale.ROOT, "  \"overheadPercent\": %s,\n  \"cpuSeconds\": %.2f,\n  \"peakHeapMB\": %.0f\n}\n",
                overheadPct == null ? "null" : String.format(Locale.ROOT, "%.2f", overheadPct), cpuSec, peakHeapBytes / 1048576.0));
        Files.writeString(Path.of(cfg.report + ".json"), js.toString(), StandardCharsets.UTF_8);
        if (cfg.saveBaseline != null) {
            Files.writeString(Path.of(cfg.saveBaseline), String.format(Locale.ROOT,
                    "{\n  \"config\": \"%s\",\n  \"opsPerSec\": %.1f\n}\n", cfg.fingerprint(), opsPerSec), StandardCharsets.UTF_8);
        }

        if (cfg.ansi) System.out.print("\033[H\033[2J");
        System.out.println("==================== DIXY STRESS TEST RESULT: " + verdict + " ====================");
        for (String[] c : checks) System.out.printf("  [%-4s] %-58s %s%n", c[1], c[0], c[2]);
        System.out.printf("  ops ok=%,d failed=%,d | %,.0f ops/s | workload %.1fs | peak R/B/W/T=%d/%d/%d/%d%n",
                ok, failed, opsPerSec, workSec, peak[0], peak[1], peak[2], peak[3]);
        if (overheadPct != null) System.out.printf("  monitoring overhead vs baseline: %.1f%%%n", overheadPct);
        System.out.println("  report: " + cfg.report + ".md / .json");
        if (verdict.equals("INCONCLUSIVE")) System.out.println("  (JVM-side checks passed; DIXY backend could not be queried - see --dixy-url / --dixy-deadlocks-path)");
        return exitCode;
    }

    long dixyAlerts() {
        return (dixyReachable && dixyBaseline >= 0 && dixyCurrent >= 0) ? dixyCurrent - dixyBaseline : -1;
    }

    static String[] chk(String name, boolean ok, String detail) { return new String[]{name, ok ? "PASS" : "FAIL", detail}; }

    static char code(Thread.State s) {
        switch (s) {
            case RUNNABLE: return 'R';
            case BLOCKED: return 'B';
            case WAITING: case TIMED_WAITING: return 'W';
            case TERMINATED: return 'T';
            default: return 'N';
        }
    }

    void render(long elapsedNanos, int[] c, String grid, long ok, double opsPerSec, long heap) {
        double el = elapsedNanos / 1e9;
        int pct = (int) Math.min(100, el * 100 / cfg.durationSec);
        int w = 40, filled = pct * w / 100;
        long a = dixyAlerts();
        StringBuilder sb = new StringBuilder();
        if (cfg.ansi) sb.append("\033[H");
        sb.append(String.format("DIXY Stress Test  [%s]  %d threads  elapsed %.1fs / %ds%n", cfg.deadlockTest ? "DEADLOCK-TEST" : "NORMAL", cfg.threads, el, cfg.durationSec));
        sb.append("[").append("#".repeat(filled)).append("-".repeat(w - filled)).append("] ").append(pct).append("%\n");
        sb.append(String.format("Threads : RUNNING %-4d BLOCKED %-4d WAITING %-4d TERMINATED %-4d (started %d/%d)%n",
                c[0], c[1], c[2], c[3], startedCount.get(), cfg.threads));
        sb.append(String.format("Ops     : ok %,d  failed %,d  %,.0f ops/s%n", ok, failedOps.sum(), opsPerSec));
        sb.append(String.format("Deadlock: JVM ground truth %d thread(s) | DIXY alerts %s%n", localDeadlockedNames.size(),
                a >= 0 ? String.valueOf(a) : "n/a (backend unreachable)"));
        sb.append(String.format("Heap    : %.0f MB   | R=running B=blocked W=waiting T=terminated%n", heap / 1048576.0));
        if (cfg.ansi) sb.append(grid.replace("\n", "\033[K\n")).append("\033[J");
        else sb.append(String.format("(compact) R=%d B=%d W=%d T=%d%n", c[0], c[1], c[2], c[3]));
        System.out.print(sb);
        System.out.flush();
    }

    // ------------------------------------------------------------- DIXY client
    static final class DixyClient {
        final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        final URI uri;

        DixyClient(String base, String path) {
            URI u = null;
            try { if (!base.equalsIgnoreCase("none")) u = URI.create(base.replaceAll("/+$", "") + path); } catch (Exception ignored) { }
            uri = u;
        }

        /** Number of deadlock records reported by DIXY, or -1 if unreachable/unparsable. */
        long fetchCount() {
            if (uri == null) return -1;
            try {
                HttpResponse<String> r = http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3)).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() / 100 != 2) return -1;
                String b = r.body().trim();
                if (b.startsWith("[")) return countArray(b, 0);
                Matcher m = Pattern.compile("\"(totalElements|count|total|deadlockCount)\"\\s*:\\s*(\\d+)").matcher(b);
                if (m.find()) return Long.parseLong(m.group(2));
                int i = b.indexOf('[');
                return i >= 0 ? countArray(b, i) : -1;
            } catch (Exception e) {
                return -1;
            }
        }

        /** Counts top-level elements (objects/scalars) of the JSON array starting at s[start]=='['. */
        static long countArray(String s, int start) {
            int depth = 0; long n = 0; boolean inStr = false, esc = false, elem = false;
            for (int i = start; i < s.length(); i++) {
                char c = s.charAt(i);
                if (inStr) { if (esc) esc = false; else if (c == '\\') esc = true; else if (c == '"') inStr = false; continue; }
                if (c == '"') { inStr = true; if (depth == 1 && !elem) { n++; elem = true; } continue; }
                if (c == '[' || c == '{') { depth++; if (depth == 2 && !elem) { n++; elem = true; } continue; }
                if (c == ']' || c == '}') { depth--; if (depth == 0) break; continue; }
                if (c == ',' && depth == 1) { elem = false; continue; }
                if (depth == 1 && !Character.isWhitespace(c) && !elem) { n++; elem = true; }
            }
            return n;
        }
    }

    public static void main(String[] args) throws Exception {
        Config cfg;
        try { cfg = Config.parse(args); }
        catch (IllegalArgumentException e) { System.err.println("Error: " + e.getMessage()); usage(); System.exit(64); return; }
        System.exit(new DixyStressTest(cfg).run());
    }
}
