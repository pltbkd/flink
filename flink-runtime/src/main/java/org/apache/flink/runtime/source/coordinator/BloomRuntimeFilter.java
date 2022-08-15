package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.RuntimeFilter;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.util.function.SerializableFunction;

import java.util.Arrays;
import java.util.List;

public class BloomRuntimeFilter<T> implements RuntimeFilter<T> {
    private final int estimatedBuildCount;
    private final int bloomFilterByteSize;
    private final byte[] filterBits;

    private final List<SerializableFunction<T, Object>> keyExtractors;
    private transient BloomFilter filter;

    public BloomRuntimeFilter(
            int estimatedBuildCount,
            int bloomFilterByteSize,
            byte[] filterBits,
            List<SerializableFunction<T, Object>> keyExtractors) {
        this.estimatedBuildCount = estimatedBuildCount;
        this.bloomFilterByteSize = bloomFilterByteSize;
        this.filterBits = filterBits;
        this.keyExtractors = keyExtractors;
    }

    @Override
    public boolean match(T value) {
        if (filter == null) {
            filter = new BloomFilter(estimatedBuildCount, bloomFilterByteSize);
            filter.setBitsLocation(MemorySegmentFactory.wrap(filterBits), 0);
        }
        int hash =
                Arrays.hashCode(
                        keyExtractors.stream().map(extractor -> extractor.apply(value)).toArray());
        return filter.testHash(hash);
    }
}
