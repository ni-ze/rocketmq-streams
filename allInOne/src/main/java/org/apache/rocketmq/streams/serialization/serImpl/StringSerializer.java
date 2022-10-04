package org.apache.rocketmq.streams.serialization.serImpl;

import org.apache.rocketmq.streams.serialization.Serializer;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {

    @Override
    public byte[] serialize(String data) {
        if (data == null) {
            return new byte[0];
        }

        return data.getBytes(StandardCharsets.UTF_8);
    }
}
