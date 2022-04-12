package com.meituan.ptubes.sdk.reader.processor;

import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.sdk.reader.bean.ClientRequest;

public class ProcessorChain implements IProcessorChain {

    private final List<IProcessor> processors = new ArrayList<>();

    public ProcessorChain(
    ) {
        this.processors.add(new GetRequestProcessor());
        this.processors.add(new SubscribtionProcessor());
        this.processors.add(new ServerInfoProcessor());
    }

    @Override
    public void process(
        ChannelHandlerContext ctx,
        ClientRequest clientRequest
    ) {
        for (IProcessor processor : this.processors) {
            if (processor.isMatch(clientRequest)) {
                processor.process(
                    ctx,
                    clientRequest
                );
            }
        }
    }
}
