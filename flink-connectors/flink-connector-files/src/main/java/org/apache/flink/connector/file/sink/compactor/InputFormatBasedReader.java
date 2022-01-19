package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;

public class InputFormatBasedReader<T> implements RecordWiseFileCompactor.Reader<T> {
    private final Path path;
    private final FileInputFormat<T> inputFormat;

    public InputFormatBasedReader(Path path, FileInputFormat<T> inputFormat) throws IOException {
        this.path = path;
        this.inputFormat = inputFormat;
        open();
    }

    private void open() throws IOException {
        long len = path.getFileSystem().getFileStatus(path).getLen();
        inputFormat.open(new FileInputSplit(0, path, 0, len, null));
    }

    @Override
    public T read() throws IOException {
        if (inputFormat.reachedEnd()) {
            return null;
        }
        return inputFormat.nextRecord(null);
    }

    @Override
    public void close() throws IOException {
        inputFormat.close();
    }

    public static class Factory<T> implements RecordWiseFileCompactor.Reader.Factory<T> {
        private final SerializableSupplierWithException<FileInputFormat<T>, IOException>
                inputFormatFactory;

        public Factory(
                SerializableSupplierWithException<FileInputFormat<T>, IOException>
                        inputFormatFactory) {
            this.inputFormatFactory = inputFormatFactory;
        }

        @Override
        public InputFormatBasedReader<T> open(Path path) throws IOException {
            return new InputFormatBasedReader<>(path, inputFormatFactory.get());
        }
    }
}
