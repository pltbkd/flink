package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class FileSinkCompactOperatorFactory
        extends AbstractStreamOperatorFactory<CommittableMessage<FileSinkCommittable>>
        implements OneInputStreamOperatorFactory<
                FileCompactRequest, CommittableMessage<FileSinkCommittable>> {

    private final FileSink<?> sink;
    private final int compactThreads;

    public FileSinkCompactOperatorFactory(FileSink<?> sink, int compactThreads) {
        this.sink = sink;
        this.compactThreads = compactThreads;
    }

    @Override
    public <T extends StreamOperator<CommittableMessage<FileSinkCommittable>>>
            T createStreamOperator(
                    StreamOperatorParameters<CommittableMessage<FileSinkCommittable>> parameters) {
        try {
            final CompactOperator<FileSinkCommittable, FileCompactRequest> compactOperator =
                    new CompactOperator<>(
                            sink.createCompactor(),
                            compactThreads,
                            TypeInformation.of(FileCompactRequest.class));
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
        return CompactOperator.class;
    }
}
