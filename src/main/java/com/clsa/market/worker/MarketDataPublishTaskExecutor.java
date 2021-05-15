package com.clsa.market.worker;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author Bill Tu
 * @Time 2021-05-15 12:42:11
 */
@Slf4j
public class MarketDataPublishTaskExecutor {
    private ThreadPoolExecutor threadPoolExecutor;

    public MarketDataPublishTaskExecutor() {
        //Create thread pool here is just one of the means to execute task async for reference, we can also use messaging queue etc.
        threadPoolExecutor = new ThreadPoolExecutor(50, 50, 10L, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(50000), new RejectedExecutionHandler() {

            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                if (!threadPoolExecutor.isShutdown()) {
                    try {
                        threadPoolExecutor.getQueue().put(r);
                    } catch (InterruptedException e) {
                        log.error("Failed to add {} into working queue", r);
                    }
                }
            }
        });
    }

    public void execute(MarketDataPublishTask marketDataPublishTask) {
        this.threadPoolExecutor.execute(marketDataPublishTask);
        log.info("{} executed in thread pool", marketDataPublishTask);
    }

    public void stop() {
        //Shutdown pool gracefully
        this.threadPoolExecutor.shutdown();
        log.info("MarketDataPublishTaskExecutor stopped");
    }
}
