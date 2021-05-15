package com.clsa.market.worker;

import com.clsa.market.model.MarketData;
import com.clsa.market.service.MarketDataProcessor;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author Bill Tu
 * @Time 2021-05-15 12:18:02
 * Consume publish queue and execute the task of publishAggregatedMarketData in thread pool
 */
@Slf4j
public class MarketDataPublisher implements Runnable {
    private Thread publishThread;
    private RateLimiter rateLimiter;
    private AtomicBoolean go;
    private MarketDataProcessor marketDataProcessor;
    private LinkedBlockingQueue<MarketData> symbolPublishQueue;
    private MarketDataPublishTaskExecutor marketDataPublishTaskExecutor;
    private final String SUICIDE_SYMBOL = "suicide";

    public MarketDataPublisher(MarketDataProcessor marketDataProcessor,
                               LinkedBlockingQueue<MarketData> symbolPublishQueue,
                               MarketDataPublishTaskExecutor marketDataPublishTaskExecutor) {
        this.publishThread = new Thread(this);
        this.publishThread.setName("thread-market-data-publisher");
        this.rateLimiter = RateLimiter.create(100);//Not called more than 100 times/second
        this.go = new AtomicBoolean(true);
        this.marketDataProcessor = marketDataProcessor;
        this.symbolPublishQueue = symbolPublishQueue;
        this.marketDataPublishTaskExecutor = marketDataPublishTaskExecutor;
    }

    public void run() {
        while (go.get()) {
            //Get token before consuming queue
            if (rateLimiter.tryAcquire()) {
                try {
                    MarketData latestMarketData = this.symbolPublishQueue.take();
                    if (latestMarketData.getSymbol().equals(SUICIDE_SYMBOL)) {
                        log.info("{} is killed proactively", this.publishThread.getName());
                        break;
                    }
                    //Create publish task
                    MarketDataPublishTask marketDataPublishTask = new MarketDataPublishTask(marketDataProcessor, latestMarketData);
                    //Use thread pool to execute marketDataPublishTask is because just in case the publishAggregatedMarketData() is not async.
                    this.marketDataPublishTaskExecutor.execute(marketDataPublishTask);
                } catch (InterruptedException e) {
                    log.error("Failed to retrieve market data from blocking queue", e);
                }

            }
        }

    }

    public void start() {
        this.publishThread.start();
        log.info("{} started.", this.publishThread.getName());
    }

    public void stop() {
        go.compareAndSet(false, true);
        MarketData suicidePoison = new MarketData();
        suicidePoison.setSymbol(SUICIDE_SYMBOL);
        this.symbolPublishQueue.offer(suicidePoison);
        log.info("{} stopped", this.publishThread.getName());
    }
}
