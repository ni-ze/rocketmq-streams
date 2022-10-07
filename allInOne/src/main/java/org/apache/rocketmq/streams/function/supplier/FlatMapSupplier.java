package org.apache.rocketmq.streams.function.supplier;

import org.apache.rocketmq.streams.function.ValueMapperAction;
import org.apache.rocketmq.streams.metadata.Context;
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;

import java.util.function.Supplier;

public class FlatMapSupplier<T, VR> implements Supplier<Processor<T>> {
    private final ValueMapperAction<? super T, ? extends Iterable<? extends VR>> valueMapperAction;


    public FlatMapSupplier(ValueMapperAction<? super T, ? extends Iterable<? extends VR>> valueMapperAction) {
        this.valueMapperAction = valueMapperAction;
    }

    @Override
    public Processor<T> get() {
        return new FlatMapProcessor<>(this.valueMapperAction);
    }


    static class FlatMapProcessor<T, VR> extends AbstractProcessor<T> {
        private final ValueMapperAction<? super T, ? extends Iterable<? extends VR>> valueMapperAction;
        private StreamContext<T> context;

        public FlatMapProcessor(ValueMapperAction<? super T, ? extends Iterable<? extends VR>> valueMapperAction) {
            this.valueMapperAction = valueMapperAction;
        }

        @Override
        public void preProcess(StreamContext<T> context) {
            this.context = context;
            this.context.init(super.getChildren());
        }

        @Override
        public void process(T data) {

            Iterable<? extends VR> converts = valueMapperAction.convert(data);

            for (VR convert : converts) {
                Context<Object, VR> vrContext = new Context<>(this.context.getKey(), convert);
                Context<Object, T> result = convert(vrContext);
                this.context.forward(result);
            }
        }
    }
}
