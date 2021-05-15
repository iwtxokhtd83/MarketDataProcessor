package com.clsa.market.worker;

import com.clsa.market.model.MarketData;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author Bill Tu
 * @Time 2021-05-15 11:29:31
 * Pick the latest market data of each symbol and put it into a queue for publishing
 */
@Slf4j
public class MarketDataPicker implements Runnable {
    private Thread pickThread;
    private AtomicBoolean go;
    private Map<String, List<MarketData>> symbolUpdateQueueMap;
    private Map<String, String> symbolLockBucket;
    private LinkedBlockingQueue<MarketData> symbolPublishQueue;
    private Map<String, RateLimiter> symbolUpdateRateLimiterMap;


    public MarketDataPicker(Map<String, RateLimiter> symbolUpdateRateLimiterMap,
                            Map<String, List<MarketData>> symbolUpdateQueueMap,
                            Map<String, String> symbolLockBucket,
                            LinkedBlockingQueue<MarketData> symbolPublishQueue) {
        this.pickThread = new Thread(this);
        this.pickThread.setName("thread-market-data-picker");
        this.go = new AtomicBoolean(true);
        this.symbolLockBucket = symbolLockBucket;
        this.symbolUpdateRateLimiterMap = symbolUpdateRateLimiterMap;
        this.symbolUpdateQueueMap = symbolUpdateQueueMap;
        this.symbolPublishQueue = symbolPublishQueue;
    }

    public void run() {
        while (go.get()) {
            try {
                Iterator<Map.Entry<String, List<MarketData>>> symbolUpdateIterator = symbolUpdateQueueMap.entrySet().iterator();
                while (symbolUpdateIterator.hasNext()) {
                    Map.Entry<String, List<MarketData>> symbolUpdateEntry = symbolUpdateIterator.next();
                    String symbol = symbolUpdateEntry.getKey();
                    List<MarketData> marketDataList = symbolUpdateEntry.getValue();
                    if (symbolUpdateRateLimiterMap.get(symbol).tryAcquire()) {
                        synchronized (symbolLockBucket.get(symbol)) {
                            Collections.sort(marketDataList, Collections.<MarketData>reverseOrder());
                            //Always publish the latest market data
                            MarketData latestMarketData = marketDataList.get(0);
                            this.symbolPublishQueue.offer(latestMarketData);
                            //Release memory by removing the old market data
                            marketDataList.clear();
                            symbolUpdateIterator.remove();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Failed to pick latest market data.", e);
            }

            //Each symbol will not have more than one update per second
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
        }
    }

    public void start() {
        this.pickThread.start();
        log.info("{} started.", this.pickThread.getName());

    }


    public void stop() {
        go.compareAndSet(false, true);
        log.info("Stopped {}", this.pickThread.getName());
    }
}
