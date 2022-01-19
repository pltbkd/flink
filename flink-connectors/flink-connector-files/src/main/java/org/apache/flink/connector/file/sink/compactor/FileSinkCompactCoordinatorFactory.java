package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class FileSinkCompactCoordinatorFactory
        extends AbstractStreamOperatorFactory<FileCompactRequest>
        implements OneInputStreamOperatorFactory<
                CommittableMessage<FileSinkCommittable>, FileCompactRequest> {

    private final FileSink<?> sink;
    private final CompactStrategy strategy;

    public FileSinkCompactCoordinatorFactory(FileSink<?> sink, CompactStrategy strategy) {
        this.sink = sink;
        this.strategy = strategy;
    }

    @Override
    public <T extends StreamOperator<FileCompactRequest>> T createStreamOperator(
            StreamOperatorParameters<FileCompactRequest> parameters) {
        try {
            CompactRequestPacker<FileSinkCommittable, FileCompactRequest> packer =
                    FileCompactRequestPacker.Builder.newBuilder()
                            .withSizeThreshold(strategy.getSizeThreshold())
                            .withMaxIntervalMs(strategy.getMaxIntervalMs())
                            .allowCrossCheckpoint(strategy.isAllowCrossCheckpoint())
                            .build();
            final CompactCoordinator<FileSinkCommittable, FileCompactRequest> compactOperator =
                    new CompactCoordinator<>(packer, sink.getCommittableSerializer());
            compactOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (T) compactOperator;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create commit operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return CompactCoordinator.class;
    }
}
