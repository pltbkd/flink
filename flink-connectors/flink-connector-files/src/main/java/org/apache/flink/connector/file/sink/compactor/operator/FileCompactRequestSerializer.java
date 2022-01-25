package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.List;

public class FileCompactRequestSerializer implements SimpleVersionedSerializer<FileCompactRequest> {
    private final SimpleVersionedSerializer<FileSinkCommittable> committableSerializer;

    private static final int MAGIC_NUMBER = 0x2fc61e19;

    public FileCompactRequestSerializer(
            SimpleVersionedSerializer<FileSinkCommittable> committableSerializer) {
        this.committableSerializer = committableSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FileCompactRequest request) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(request, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public FileCompactRequest deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        switch (version) {
            case 1:
                validateMagicNumber(in);
                return deserializeV1(in);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private void serializeV1(FileCompactRequest request, DataOutputSerializer out)
            throws IOException {
        out.writeUTF(request.getBucketId());
        SimpleVersionedSerialization.writeVersionAndSerializeList(
                committableSerializer, request.getCommittableList(), out);
    }

    private FileCompactRequest deserializeV1(DataInputDeserializer in) throws IOException {
        String bucketId = in.readUTF();
        List<FileSinkCommittable> committableList =
                SimpleVersionedSerialization.readVersionAndDeserializeList(
                        committableSerializer, in);
        return new FileCompactRequest(bucketId, committableList);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
