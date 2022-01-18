package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.compactor.FileCompactor.FSOutputStreamBasedCompactor;
import org.apache.flink.connector.file.table.stream.compact.CompactContext;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;
import java.util.List;

public class ReadingElementsFileCompactor<InputT> implements FSOutputStreamBasedCompactor {
    private final Configuration config;
    private final SerializableSupplierWithException<CompactReader.Factory<InputT>, IOException>
            elementReaderFactorySupplier;
    private final Encoder<InputT> encoder;

    public ReadingElementsFileCompactor(
            Configuration config,
            SerializableSupplierWithException<CompactReader.Factory<InputT>, IOException>
                    elementReaderFactorySupplier,
            Encoder<InputT> encoder) {
        this.config = config;
        this.elementReaderFactorySupplier = elementReaderFactorySupplier;
        this.encoder = encoder;
    }

    @Override
    public void compact(List<Path> inputFiles, FSDataOutputStream output) throws IOException {
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
                encoder.encode(record, output);
            }
        }
    }
}
