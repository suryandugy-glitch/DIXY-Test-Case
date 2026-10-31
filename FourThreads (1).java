import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 4 threads:
 *   - Deadlock-1 and Deadlock-2 lock A/B in opposite order -> they deadlock and never finish
 *   - Worker-1 and Worker-2 lock C/D in the same order      -> they finish normally
 *
 * This program does NOT detect the deadlock itself. When the two normal threads are done,
 * main prints "Hello World" as the last line and the program ends.
 * (The deadlocked threads are daemons, so they don't keep the JVM alive.)
 *
 * Run it under DIXY (-javaagent:dixy-agent.jar) and DIXY should report the A/B deadlock
 * before "Hello World" appears.
 */
public class FourThreads {
    static final ReentrantLock A = new ReentrantLock(), B = new ReentrantLock();
    static final ReentrantLock C = new ReentrantLock(), D = new ReentrantLock();
    static final CountDownLatch bothHoldFirst = new CountDownLatch(2);

    static void deadlockWorker(ReentrantLock first, ReentrantLock second) throws InterruptedException {
        first.lock();
        bothHoldFirst.countDown();
        bothHoldFirst.await();      // both threads hold their first lock before asking for the second
        second.lock();              // blocks forever -> DEADLOCK
    }

    static void normalWorker(ReentrantLock first, ReentrantLock second) {
        for (int i = 0; i < 3; i++) {
            first.lock();
            try {
                second.lock();
                try { Thread.sleep(100); } catch (InterruptedException ignored) { } finally { second.unlock(); }
            } finally { first.unlock(); }
        }
        System.out.println(Thread.currentThread().getName() + " finished");
    }

    public static void main(String[] args) throws Exception {
        Thread d1 = new Thread(() -> { try { deadlockWorker(A, B); } catch (InterruptedException ignored) { } }, "Deadlock-1");
        Thread d2 = new Thread(() -> { try { deadlockWorker(B, A); } catch (InterruptedException ignored) { } }, "Deadlock-2");
        Thread w1 = new Thread(() -> normalWorker(C, D), "Worker-1");
        Thread w2 = new Thread(() -> normalWorker(C, D), "Worker-2");
        d1.setDaemon(true);
        d2.setDaemon(true);

        d1.start(); d2.start(); w1.start(); w2.start();

        w1.join();
        w2.join();
        Thread.sleep(3000);         // short pause so a monitoring agent (DIXY) has time to observe the deadlock

        System.out.println("Hello World");
    }
}
