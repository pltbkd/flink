package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.compactor.FileCompactor.FileCompactReader;
import org.apache.flink.connector.file.table.stream.compact.CompactContext;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;

public class FileSinkCommittableReader<InputT> implements FileCompactReader<InputT> {
    private final CompactReader<InputT> elementReader;

    public FileSinkCommittableReader(CompactReader<InputT> elementReader) {
        this.elementReader = elementReader;
    }

    @Override
    public InputT read() throws IOException {
        return elementReader.read();
    }

    @Override
    public void close() throws IOException {
        elementReader.close();
    }

    public static class Factory<InputT> implements FileCompactReader.Factory<InputT> {
        private final Configuration config;
        private final SerializableSupplierWithException<CompactReader.Factory<InputT>, IOException>
                elementReaderFactorySupplier;

        public Factory(
                Configuration config,
                SerializableSupplierWithException<CompactReader.Factory<InputT>, IOException>
                        elementReaderFactorySupplier) {
            this.config = config;
            this.elementReaderFactorySupplier = elementReaderFactorySupplier;
        }

        @Override
        public FileCompactReader<InputT> create(
                FileCompactRequest request, PendingFileRecoverable pendingFileRecoverable)
                throws IOException {
            // do not cache this factory, inputFormat is reused
            CompactReader.Factory<InputT> elementReaderFactory = elementReaderFactorySupplier.get();
            Path path = FileCompactorUtil.getPath(pendingFileRecoverable);
            CompactReader<InputT> elementReader =
                    elementReaderFactory.create(
                            CompactContext.create(
                                    config, path.getFileSystem(), request.getBucketId(), path));
            return new FileSinkCommittableReader<>(elementReader);
        }
    }
}
