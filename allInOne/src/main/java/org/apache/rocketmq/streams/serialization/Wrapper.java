package org.apache.rocketmq.streams.serialization;

import org.apache.rocketmq.streams.serialization.deImpl.StringDeserializer;
import org.apache.rocketmq.streams.serialization.serImpl.StringSerializer;

public class Wrapper {

    static class WrapperSerde<T> implements Serde<T> {
        private Serializer<T> serializer;

        private Deserializer<T> deserializer;

        public WrapperSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public Serializer<T> serializer() {
            return this.serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this.deserializer;
        }

        @Override
        public void close() {
            serializer.close();
            deserializer.close();
        }
    }

    public static class StringSerde extends WrapperSerde<String> {
        public StringSerde() {
            super(new StringSerializer(), new StringDeserializer());
        }
    }

}
