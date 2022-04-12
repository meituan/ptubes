package com.meituan.ptubes.reader.container;

public class DemoMain {

    public static void main(String[] args) {
        /**
         * 1. In the future, there needs to be a daemon thread, which is responsible for monitoring the thread running status of the producer (4 threads)
         * and session (2 threads). If there is a problem with one of the threads, you should try to restore or close
         * and recycle the resources of the thread group.
         * 2. The two threads of the session are updated to thread multiplexing in the thread pool similar to netty.
         * Each session is allocated extractorThread and transmitterThread by the two thread pools.
         */
        Container container = new Container();
        container.start();
        System.out.println("ptubes Container started.");
    }
}
