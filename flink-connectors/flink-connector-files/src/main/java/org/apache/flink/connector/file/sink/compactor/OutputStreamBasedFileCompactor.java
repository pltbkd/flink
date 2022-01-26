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
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedCompactingFileWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Base class for {@link FileCompactor} implementations that use the {@link
 * OutputStreamBasedCompactingFileWriter}.
 */
public abstract class OutputStreamBasedFileCompactor implements FileCompactor {
    @Override
    public final CompactingFileWriter.Type getWriterType() {
        return CompactingFileWriter.Type.OUTPUT_STREAM;
    }

    @Override
    public void compact(List<Path> inputFiles, CompactingFileWriter writer) throws Exception {
        // The outputStream returned by OutputStreamBasedCompactingFileWriter#asOutputStream should
        // not be closed here.
        CloseShieldOutputStream outputStream =
                new CloseShieldOutputStream(
                        ((OutputStreamBasedCompactingFileWriter) writer).asOutputStream());
        doCompact(inputFiles, outputStream);
    }

    protected abstract void doCompact(List<Path> inputFiles, OutputStream outputStream)
            throws Exception;

    // A utility class to prevent the enclosing OutputStream being closed.
    private static class CloseShieldOutputStream extends OutputStream {
        private final OutputStream out;

        public CloseShieldOutputStream(OutputStream out) {
            this.out = out;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] buffer) throws IOException {
            out.write(buffer);
        }

        @Override
        public void write(byte[] buffer, int off, int len) throws IOException {
            out.write(buffer, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            // Do not actually close the internal stream.
        }
    }
}
