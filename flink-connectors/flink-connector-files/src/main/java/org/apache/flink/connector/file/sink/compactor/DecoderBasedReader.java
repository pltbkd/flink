package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor.Reader;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class DecoderBasedReader<T> implements RecordWiseFileCompactor.Reader<T> {
    private final Decoder<T> decoder;
    private final FSDataInputStream input;

    public DecoderBasedReader(Path path, Decoder<T> decoder) throws IOException {
        this.decoder = decoder;
        this.input = path.getFileSystem().open(path);
    }

    @Override
    public T read() throws IOException {
        return decoder.decodeNext(input);
    }

    @Override
    public void close() throws Exception {
        input.close();
    }

    public interface Decoder<T> extends Serializable {
        T decodeNext(InputStream input) throws IOException;
    }

    public static class Factory<T> implements RecordWiseFileCompactor.Reader.Factory<T> {
        private final Decoder<T> decoder;

        public Factory(Decoder<T> decoder) {
            this.decoder = decoder;
        }

        @Override
        public DecoderBasedReader<T> open(Path path) throws IOException {
            return new DecoderBasedReader<>(path, decoder);
        }
    }
}
