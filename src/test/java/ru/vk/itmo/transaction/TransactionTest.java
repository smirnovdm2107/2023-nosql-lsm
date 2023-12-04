package ru.vk.itmo.transaction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import ru.vk.itmo.BaseTest;
import ru.vk.itmo.Dao;
import ru.vk.itmo.DaoTest;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.transaction.LockTransaction;
import ru.vk.itmo.smirnovdmitrii.transaction.Transaction;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionTest extends BaseTest {

    private static Transaction.TransactionGroup<String> group = new Transaction.TransactionGroup<>();

    private Transaction<String, Entry<String>> createTransaction(final Dao<String, Entry<String>> dao) {
        return new LockTransaction<>(dao, group);
    }

    @BeforeEach
    public void renewGroup() {
        group = new Transaction.TransactionGroup<>();
    }

    @DaoTest(stage = 5)
    void testParallelRead(final Dao<String, Entry<String>> dao) throws Exception {
        final int count = 10;
        final List<Entry<String>> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            dao.upsert(entryAt(i));
            entries.add(entryAt(i));
        }

        final ParallelTask task = (_) -> {
            final Transaction<String, Entry<String>> transaction = createTransaction(dao);
            final List<Entry<String>> results = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                sleep(10);
                results.add(transaction.get(keyAt(i)));
            }
            assertSame(results.listIterator(), entries);
            transaction.commit();
        };
        final int taskCount = 10;
        runInParallel(taskCount, task).close();
    }

    @DaoTest(stage = 5)
    void testParallelWrite(final Dao<String, Entry<String>> dao) throws Exception {
        final AtomicInteger successed = new AtomicInteger(0);
        final AtomicInteger failed = new AtomicInteger(0);
        final ParallelTask task = (_) -> {
            final Transaction<String, Entry<String>> transaction = createTransaction(dao);
            try {
                transaction.upsert(entryAt(0));
                sleep(20);
                transaction.commit();
                successed.incrementAndGet();
            } catch (final ConcurrentModificationException ignored) {
                failed.incrementAndGet();
            }
        };
        final int taskCount = 10;
        runInParallel(taskCount, task).close();
        Assertions.assertEquals(successed.get(), 1);
        Assertions.assertEquals(failed.get(), taskCount - 1);
        Assertions.assertEquals(dao.get(keyAt(0)), entryAt(0));
    }

    @DaoTest(stage = 5)
    void testSingleThreadReadWrite(final Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entryAt(0));
        dao.upsert(entryAt(1));
        // transactional swap
        Transaction<String, Entry<String>> transaction = createTransaction(dao);
        Entry<String> entry1 = transaction.get(keyAt(0));
        Entry<String> entry2 = transaction.get(keyAt(1));

        Entry<String> newEntry1 = entry(entry1.key(), entry2.value());
        Entry<String> newEntry2 = entry(entry2.key(), entry2.value());
        transaction.upsert(newEntry1);
        transaction.upsert(newEntry2);
        transaction.commit();

        Assertions.assertEquals(dao.get(keyAt(0)), newEntry1);
        Assertions.assertEquals(dao.get(keyAt(1)), newEntry2);


        // read after write
        transaction = createTransaction(dao);
        transaction.upsert(entryAt(2));
        Entry<String> entry = transaction.get(keyAt(2));
        transaction.commit();

        Assertions.assertEquals(entry, entryAt(2));
    }


    @DaoTest(stage = 5)
    void testParallelSwap(final Dao<String, Entry<String>> dao) throws Exception {
        dao.upsert(entryAt(0));
        dao.upsert(entryAt(1));
        final AtomicInteger counter = new AtomicInteger(0);
        final ParallelTask task = (i) -> {
            try {
                Transaction<String, Entry<String>> transaction = createTransaction(dao);
                Entry<String> entry1 = transaction.get(keyAt(0));
                Entry<String> entry2 = transaction.get(keyAt(1));

                Entry<String> newEntry1 = entry(entry1.key(), entry2.value());
                Entry<String> newEntry2 = entry(entry2.key(), entry2.value());
                // wait for all to start reading
                sleep(100);
                transaction.upsert(newEntry1);
                transaction.upsert(newEntry2);
                transaction.commit();
            } catch (final ConcurrentModificationException ignored) {
                counter.incrementAndGet();
            }
        };
        final int tasks = 20;
        runInParallel(tasks, task).close();
        // all failed
        Assertions.assertEquals(tasks, counter.get());
        Assertions.assertEquals(entryAt(0),dao.get(keyAt(0)) );
        Assertions.assertEquals(entryAt(1), dao.get(keyAt(1)) );
    }

}
