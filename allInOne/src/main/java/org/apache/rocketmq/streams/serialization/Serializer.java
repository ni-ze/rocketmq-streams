package org.apache.rocketmq.streams.serialization;

import java.io.Closeable;

public interface Serializer<T> extends Closeable {
    byte[] serialize(T data);

    @Override
    default void close() {
        // intentionally left blank
    }
}
