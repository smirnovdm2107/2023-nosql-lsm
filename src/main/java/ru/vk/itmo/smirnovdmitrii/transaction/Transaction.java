package ru.vk.itmo.smirnovdmitrii.transaction;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.UpgradableReadWriteLock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface Transaction<T, E extends Entry<T>> {

    E get(T t);

    void upsert(E e);

    void commit();

    class TransactionGroup<T> {
        public final Map<T, UpgradableReadWriteLock> sharedMap = new ConcurrentHashMap<>();
    }
}
