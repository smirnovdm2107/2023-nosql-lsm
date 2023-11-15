package ru.vk.itmo.smirnovdmitrii.inmemory;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class SkipListMemtable implements Memtable {
    public final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return get(null, null);
    }

    private final AtomicLong currentSize = new AtomicLong(0);

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        final Map<MemorySegment, Entry<MemorySegment>> map;
        if (from == null && to == null) {
            map = storage;
        } else if (from == null) {
            map = storage.headMap(to);
        } else if (to == null) {
            map = storage.tailMap(from);
        } else {
            map = storage.subMap(from, to);
        }
        return map.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public long size() {
        return currentSize.get();
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {

        final Entry<MemorySegment> oldValue = storage.put(entry.key(), entry);
        long sizeAdd = 0;
        final long oldValueSize;
        if (oldValue == null) {
            sizeAdd += entry.key().byteSize();
            oldValueSize = 0;
        } else {
            oldValueSize = oldValue.value() == null ? 0 : oldValue.value().byteSize();
        }
        final long newValueSize = entry.value() == null ? 0 : entry.value().byteSize();
        sizeAdd += newValueSize - oldValueSize;
        currentSize.addAndGet(sizeAdd);
    }

}