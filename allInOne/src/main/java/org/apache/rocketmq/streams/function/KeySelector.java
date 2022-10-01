package org.apache.rocketmq.streams.function;

@FunctionalInterface
public interface KeySelector<T, KEY> {
    KEY select(T data);
}
