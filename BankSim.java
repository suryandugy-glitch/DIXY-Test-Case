import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 250-thread bank simulation.
 * Each thread repeatedly transfers money between random accounts.
 * Locks are taken in account-id order (no deadlock). At the end the total
 * money must equal the starting total, proving the threads synchronized correctly.
 *
 * Usage: java BankSim [threads=250] [seconds=10] [accounts=20]
 */
public class BankSim {
    static final class Account {
        final int id; long balance; final ReentrantLock lock = new ReentrantLock();
        Account(int id, long b) { this.id = id; this.balance = b; }
    }

    public static void main(String[] args) throws Exception {
        int nThreads = args.length > 0 ? Integer.parseInt(args[0]) : 250;
        int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        int nAccounts = args.length > 2 ? Integer.parseInt(args[2]) : 20;

        Account[] accounts = new Account[nAccounts];
        for (int i = 0; i < nAccounts; i++) accounts[i] = new Account(i, 10_000);
        final long startTotal = 10_000L * nAccounts;

        LongAdder transfers = new LongAdder(), rejected = new LongAdder();
        long deadline = System.nanoTime() + seconds * 1_000_000_000L;
        Thread[] threads = new Thread[nThreads];

        for (int t = 0; t < nThreads; t++) {
            threads[t] = new Thread(() -> {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                while (System.nanoTime() < deadline) {
                    int a = r.nextInt(nAccounts), b = r.nextInt(nAccounts);
                    if (a == b) continue;
                    Account first = accounts[Math.min(a, b)], second = accounts[Math.max(a, b)];
                    Account from = accounts[a], to = accounts[b];
                    long amount = 1 + r.nextInt(500);
                    first.lock.lock();
                    try {
                        second.lock.lock();
                        try {
                            if (from.balance >= amount) { from.balance -= amount; to.balance += amount; transfers.increment(); }
                            else rejected.increment();
                        } finally { second.lock.unlock(); }
                    } finally { first.lock.unlock(); }
                    if (r.nextInt(20) == 0) { try { Thread.sleep(1); } catch (InterruptedException e) { return; } }
                }
            }, "teller-" + t);
        }

        long t0 = System.nanoTime();
        for (Thread t : threads) t.start();

        while (true) {
            Thread.sleep(1000);
            int run = 0, blk = 0, wait = 0, term = 0;
            for (Thread t : threads) {
                switch (t.getState()) {
                    case RUNNABLE: run++; break;
                    case BLOCKED: blk++; break;
                    case WAITING: case TIMED_WAITING: wait++; break;
                    case TERMINATED: term++; break;
                    default: break;
                }
            }
            System.out.printf("[%4.1fs] threads=%d RUNNING=%d BLOCKED=%d WAITING=%d TERMINATED=%d | transfers=%,d rejected=%,d%n",
                    (System.nanoTime() - t0) / 1e9, nThreads, run, blk, wait, term, transfers.sum(), rejected.sum());
            if (term == nThreads) break;
        }
        for (Thread t : threads) t.join();

        long total = 0;
        for (Account a : accounts) total += a.balance;
        System.out.printf("%nThreads finished: %d/%d%nTransfers: %,d (rejected for low funds: %,d)%n",
                nThreads, nThreads, transfers.sum(), rejected.sum());
        System.out.printf("Total money: start=%,d end=%,d -> %s%n", startTotal, total, total == startTotal ? "CORRECT" : "MONEY LOST (race condition!)");
        System.exit(total == startTotal ? 0 : 1);
    }
}
