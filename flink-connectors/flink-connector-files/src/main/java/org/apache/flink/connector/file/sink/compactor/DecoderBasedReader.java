/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * A {@link RecordWiseFileCompactor.Reader} implementation that reads the file as an {@link
 * FSDataInputStream} and decodes the record with the {@link Decoder}.
 */
public class DecoderBasedReader<T> implements RecordWiseFileCompactor.Reader<T> {
    private final Decoder<T> decoder;
    private final FSDataInputStream input;

    public DecoderBasedReader(Path path, Decoder<T> decoder) throws IOException {
        this.decoder = decoder;
        this.input = path.getFileSystem().open(path);
    }

    @Override
    public T read() throws IOException {
        if (input.available() > 0) {
            return decoder.decodeNext(input);
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        input.close();
    }

    /**
     * A {@link Decoder} to decode the file content into the actual records.
     *
     * <p>A {@link Decoder} is generally the reverse of a {@link
     * org.apache.flink.api.common.serialization.Encoder}.
     *
     * @param <T> Thy type of the records the reader is reading.
     */
    public interface Decoder<T> extends Serializable {
        /**
         * @return The next record that decoded from the InputStream, or null if no more available.
         */
        T decodeNext(InputStream input) throws IOException;
    }

    /** Factory for {@link DecoderBasedReader}. */
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
