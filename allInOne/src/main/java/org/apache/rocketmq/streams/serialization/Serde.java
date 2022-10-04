package org.apache.rocketmq.streams.serialization;

import java.io.Closeable;

public interface Serde<T> extends Closeable {

    @Override
    default void close() {
    }

    Serializer<T> serializer();

    Deserializer<T> deserializer();

}
