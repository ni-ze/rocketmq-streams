package org.apache.rocketmq.streams.serialization.deImpl;

import org.apache.rocketmq.streams.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class StringDeserializer implements Deserializer<String> {
    @Override
    public String deserialize(byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return new String(data, StandardCharsets.UTF_8);
        } catch (Throwable t) {
            throw new UnsupportedOperationException("encode data to string error.", t);
        }
    }
}
