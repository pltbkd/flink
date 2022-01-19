package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.stream.compact.CompactContext;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;
import java.util.List;

public abstract class ReadingElementsFileCompactorBase<InputT, OutputT>
        implements FileCompactor<OutputT> {
    private final Configuration config;
    private final SerializableSupplierWithException<CompactReader.Factory<InputT>, IOException>
            elementReaderFactorySupplier;

    public ReadingElementsFileCompactorBase(
            Configuration config,
            SerializableSupplierWithException<CompactReader.Factory<InputT>, IOException>
                    elementReaderFactorySupplier) {
        this.config = config;
        this.elementReaderFactorySupplier = elementReaderFactorySupplier;
    }

    @Override
    public void compact(List<Path> inputFiles, OutputT output) throws IOException {
        for (Path path : inputFiles) {
            // bucketId is not used here
            String bucketId = null;
            // do not cache this factory, inputFormat is reused
            CompactReader.Factory<InputT> elementReaderFactory = elementReaderFactorySupplier.get();
            CompactReader<InputT> reader =
                    elementReaderFactory.create(
                            CompactContext.create(config, path.getFileSystem(), bucketId, path));

            InputT record;
            while ((record = reader.read()) != null) {
                doWrite(record, output);
            }
        }
    }

    protected abstract void doWrite(InputT record, OutputT output) throws IOException;

    public static class ReadingElementsFileCompactor<InputT>
            extends ReadingElementsFileCompactorBase<InputT, FSDataOutputStream> {
        private final Encoder<InputT> encoder;

        public ReadingElementsFileCompactor(
                Configuration config,
                SerializableSupplierWithException<CompactReader.Factory<InputT>, IOException>
                        elementReaderFactorySupplier,
                Encoder<InputT> encoder) {
            super(config, elementReaderFactorySupplier);
            this.encoder = encoder;
        }

        @Override
        protected void doWrite(InputT record, FSDataOutputStream output) throws IOException {
            encoder.encode(record, output);
        }
    }

    public static class ReadingElementsInProgressFileCompactor<InputT>
            extends ReadingElementsFileCompactorBase<InputT, InProgressFileWriter<InputT, String>> {

        public ReadingElementsInProgressFileCompactor(
                Configuration config,
                SerializableSupplierWithException<CompactReader.Factory<InputT>, IOException>
                        elementReaderFactorySupplier) {
            super(config, elementReaderFactorySupplier);
        }

        @Override
        protected void doWrite(InputT record, InProgressFileWriter<InputT, String> output)
                throws IOException {
            output.write(record, System.currentTimeMillis());
        }
    }
}
