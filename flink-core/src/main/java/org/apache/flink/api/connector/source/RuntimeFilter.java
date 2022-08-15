package org.apache.flink.api.connector.source;

import java.io.Serializable;

public interface RuntimeFilter<T> extends Serializable {
    boolean match(T out);
}
