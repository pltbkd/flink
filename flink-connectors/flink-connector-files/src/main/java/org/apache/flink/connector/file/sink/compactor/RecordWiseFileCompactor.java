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

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.RecordWiseCompactingFileWriter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * A {@link FileCompactor} implementation that reads input files with a {@link Reader} and writes
 * with the {@link RecordWiseCompactingFileWriter}.
 */
public class RecordWiseFileCompactor<IN> implements FileCompactor {
    private final Reader.Factory<IN> readerFactory;

    public RecordWiseFileCompactor(Reader.Factory<IN> readerFactory) {
        this.readerFactory = readerFactory;
    }

    @Override
    public final CompactingFileWriter.Type getWriterType() {
        return CompactingFileWriter.Type.RECORD_WISE;
    }

    @Override
    public void compact(List<Path> inputFiles, CompactingFileWriter writer) throws Exception {
        RecordWiseCompactingFileWriter<IN> recordWriter =
                (RecordWiseCompactingFileWriter<IN>) writer;
        for (Path input : inputFiles) {
            try (Reader<IN> reader = readerFactory.open(input)) {
                IN elem;
                while ((elem = reader.read()) != null) {
                    recordWriter.write(elem);
                }
            }
        }
    }

    /**
     * The reader that reads record from the compacting files.
     *
     * @param <T> Thy type of the records that is read.
     */
    public interface Reader<T> extends AutoCloseable {

        /** @return The next record, or null if no more available. */
        T read() throws IOException;

        /**
         * Factory for {@link Reader}.
         *
         * @param <T> Thy type of the records that is read.
         */
        interface Factory<T> extends Serializable {
            /**
             * @return A reader that reads elements from the given file.
             * @throws IOException Thrown if an I/O error occurs when opening the file.
             */
            Reader<T> open(Path path) throws IOException;
        }
    }
}
