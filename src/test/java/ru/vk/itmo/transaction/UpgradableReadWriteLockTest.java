package ru.vk.itmo.transaction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import ru.vk.itmo.BaseTest;
import ru.vk.itmo.smirnovdmitrii.util.ReentrantUpgradableReadWriteLock;
import ru.vk.itmo.smirnovdmitrii.util.UpgradableReadWriteLock;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class UpgradableReadWriteLockTest extends BaseTest {


    @Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest
    @ArgumentsSource(LockProvider.class)
    @interface LockTest {

    }

    static class LockProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
            return Stream.of(Arguments.of(new ReentrantUpgradableReadWriteLock()));
        }
    }


    @LockTest
    void testReadWriteSingleThread(final ReentrantUpgradableReadWriteLock lock) {
        // write after read
        if (lock.tryReadLock()) {
                if (lock.tryWriteLock()) {
                        lock.writeUnlock();
                } else {
                    Assertions.fail();
                }
                lock.readUnlock();
        } else {
            Assertions.fail();
        }
        // read after write
        if (lock.tryWriteLock()) {
            if (lock.tryReadLock()) {
                lock.readUnlock();
            } else {
                Assertions.fail();
            }
            lock.writeUnlock();
        } else {
            Assertions.fail();
        }
    }
    @LockTest
    void testLockAfterLock(UpgradableReadWriteLock lock) throws Exception {
        if (lock.tryWriteLock()) {
            lock.writeUnlock();
        } else {
            Assertions.fail();
        }

        if (lock.tryWriteLock()) {
            lock.writeUnlock();
        } else {
            Assertions.fail();
        }
        try {
            lock.writeUnlock();
            Assertions.fail();
        } catch (final IllegalMonitorStateException ignored) {
        }
        lock = new ReentrantUpgradableReadWriteLock();
        if (lock.tryReadLock()) {
            lock.readUnlock();
        } else {
            Assertions.fail();
        }

        if (lock.tryReadLock()) {
            lock.readUnlock();
        } else {
            Assertions.fail();
        }
        try {
            lock.writeUnlock();
            Assertions.fail();
        } catch (final IllegalMonitorStateException ignored) {
        }
    }


    @LockTest
    void testConcurrent(final UpgradableReadWriteLock lock) throws Exception {
        final int taskCount = 10;

        final AtomicInteger writers = new AtomicInteger(0);
        ParallelTask task = (i) -> {
            if (lock.tryWriteLock()) {
                try {
                    writers.incrementAndGet();
                    sleep(10);
                } finally {
                    lock.writeUnlock();
                }
            }
        };

        runInParallel(taskCount, task).close();
        Assertions.assertEquals(1, writers.get());


        writers.set(0);
        final AtomicInteger readers = new AtomicInteger(0);
        task = (i) -> {
            if (lock.tryReadLock()) {
                try {
                    readers.incrementAndGet();
                    sleep(100);
                    if (lock.tryWriteLock()) {
                        try {
                            writers.getAndIncrement();
                            sleep(10);
                        } finally {
                            lock.writeUnlock();
                        }
                    } else {
                        sleep(10);
                    }
                } finally {
                    lock.readUnlock();
                }
            }
        };

        runInParallel(taskCount, task).close();
        Assertions.assertEquals(taskCount, readers.get());
        Assertions.assertEquals(0, writers.get());

    }

    @LockTest
    void testReadWhileWrite(final UpgradableReadWriteLock lock) {
        final AtomicInteger counter = new AtomicInteger(0);
        final Runnable task = () -> {
            if (lock.tryReadLock()) {
                counter.incrementAndGet();
                if (lock.tryWriteLock()) {
                    sleep(100);
                    lock.writeUnlock();
                }
                lock.readUnlock();
            }
        };
        final int taskCount = 10;
        final ExecutorService service = Executors.newFixedThreadPool(taskCount);
        service.execute(task);
        sleep(10);
        for (int i = 0; i < taskCount - 1; i++) {
            service.execute(task);
        }
        service.shutdownNow();
        service.close();
        Assertions.assertEquals( 1, counter.get());
    }

}
