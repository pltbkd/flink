package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSinkCommittable;

import java.io.Serializable;
import java.util.List;

public class FileCompactRequest implements Serializable {
    private final String bucketId;
    private final List<FileSinkCommittable> committableList;

    public FileCompactRequest(String bucketId, List<FileSinkCommittable> committableList) {
        this.bucketId = bucketId;
        this.committableList = committableList;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Iterable<FileSinkCommittable> getCommittable() {
        return committableList;
    }
}
