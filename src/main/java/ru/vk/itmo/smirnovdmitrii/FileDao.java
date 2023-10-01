package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.OutMemoryDao;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

public class FileDao implements OutMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final String SS_TABLE_NAME = "ss_table";
    private static final String SS_TABLE_OFFSETS_NAME = "ss_table_offsets";
    private final Path ssTablePath;
    private final Path ssTableOffsetsPath;

    private final Comparator<MemorySegment> comparator = new MemorySegmentComparator();

    public FileDao(final Path basePath) {
        this.ssTablePath = basePath.resolve(SS_TABLE_NAME);
        this.ssTableOffsetsPath = basePath.resolve(SS_TABLE_OFFSETS_NAME);
        try {
            Files.createDirectories(basePath);
            createFileIfNotExists(ssTablePath);
            createFileIfNotExists(ssTableOffsetsPath);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createFileIfNotExists(final Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        try (FileChannel ssTableChannel = FileChannel.open(ssTablePath,
                StandardOpenOption.READ)) {
            final MemorySegment storage = ssTableChannel.map(
                    FileChannel.MapMode.READ_ONLY, 0, ssTableChannel.size(), Arena.ofAuto());
            try (FileChannel ssTableOffsetsChannel = FileChannel.open(ssTableOffsetsPath,
                    StandardOpenOption.READ)) {
                final MemorySegment offsets = ssTableOffsetsChannel.map(
                        FileChannel.MapMode.READ_ONLY, 0, ssTableOffsetsChannel.size(), Arena.ofAuto());
                return binarySearch(key, storage, offsets);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Exception while getting value from disk.", e);
        }
    }

    private Entry<MemorySegment> binarySearch(
            final MemorySegment key,
            final MemorySegment storage,
            final MemorySegment offsets
    ) {
        long left = -1;
        long right = offsets.byteSize() / Long.BYTES;
        while (left < right - 1) {
            long midst = (left + right) / 2;
            long offset = offsets.get(ValueLayout.JAVA_LONG_UNALIGNED, midst * Long.BYTES);

            final long currentKeySize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            final MemorySegment currentKey = storage.asSlice(offset, currentKeySize);

            final int compareResult = comparator.compare(key, currentKey);
            if (compareResult == 0) {
                offset += currentKeySize;
                final long currentValueSize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                final MemorySegment value = storage.asSlice(offset, currentValueSize);
                return new BaseEntry<>(key, value);
            } else if (compareResult > 0) {
                left = midst;
            } else {
                right = midst;
            }
        }
        return null;
    }

    /*
        Saves storage with one byte block per element. One block is
        [JAVA_LONG_UNALIGNED] key_size [bytes] key [JAVA_LONG_UNALIGNED] value_size [bytes] value (without spaces).

        Also saves offsets in same order.
     */
    @Override
    public void save(final Map<MemorySegment, Entry<MemorySegment>> storage) {
        Objects.requireNonNull(storage, "storage must be not null");
        if (storage.isEmpty()) {
            return;
        }
        try (FileChannel ssTableChannel = FileChannel.open(ssTablePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ)) {
            long appendSize = 0;
            final Collection<Entry<MemorySegment>> values = storage.values();
            for (final Entry<MemorySegment> entry : values) {
                appendSize += entry.value().byteSize() + entry.key().byteSize() + 2L * Long.BYTES;
            }
            final MemorySegment mappedSsTable = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, appendSize, Arena.ofAuto());
            long ssTableOffset = 0;
            try (FileChannel ssTableOffsetsChannel = FileChannel.open(ssTableOffsetsPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ)) {
                final MemorySegment mappedSsTableOffsets = ssTableOffsetsChannel.map(
                        FileChannel.MapMode.READ_WRITE, 0, (long) values.size() * Long.BYTES, Arena.ofAuto());
                int ssTableOffsetsOffset = 0;
                for (final Entry<MemorySegment> entry : values) {
                    mappedSsTableOffsets.set(ValueLayout.JAVA_LONG_UNALIGNED, ssTableOffsetsOffset, ssTableOffset);
                    ssTableOffsetsOffset += Long.BYTES;
                    ssTableOffset = write(entry.key(), mappedSsTable, ssTableOffset);
                    ssTableOffset = write(entry.value(), mappedSsTable, ssTableOffset);
                }
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Exception while saving value to disk.", e);
        }
    }

    private long write(final MemorySegment from, final MemorySegment to, final long offset) {
        final long fromByteSize = from.byteSize();
        to.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, fromByteSize);
        final long valueOffset = offset + Long.BYTES;
        MemorySegment.copy(from, 0, to, valueOffset, fromByteSize);
        return valueOffset + from.byteSize();
    }
}
