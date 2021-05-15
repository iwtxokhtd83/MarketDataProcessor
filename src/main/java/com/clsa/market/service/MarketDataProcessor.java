package com.clsa.market.service;

import com.clsa.market.model.MarketData;
import com.clsa.market.worker.MarketDataPicker;
import com.clsa.market.worker.MarketDataPublishTaskExecutor;
import com.clsa.market.worker.MarketDataPublisher;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Author Bill Tu
 * @Time 2021-05-15 10:56:47
 */
@Slf4j
public class MarketDataProcessor {
    private final Map<String, List<MarketData>> symbolUpdateQueueMap = new ConcurrentHashMap<String, List<MarketData>>();
    private final Map<String, RateLimiter> symbolUpdateRateLimiterMap = new ConcurrentHashMap<String, RateLimiter>();
    private final Map<String, String> symbolLockBucket = new ConcurrentHashMap<String, String>();
    private final LinkedBlockingQueue<MarketData> symbolPublishQueue = new LinkedBlockingQueue<MarketData>();

    private MarketDataPicker marketDataPicker;
    private MarketDataPublisher marketDataPublisher;
    private MarketDataPublishTaskExecutor marketDataPublishTaskExecutor;


    public MarketDataProcessor() {
        this.marketDataPicker = new MarketDataPicker(symbolUpdateRateLimiterMap, symbolUpdateQueueMap, symbolLockBucket, symbolPublishQueue);
        this.marketDataPublishTaskExecutor = new MarketDataPublishTaskExecutor();
        this.marketDataPublisher = new MarketDataPublisher(this, symbolPublishQueue, marketDataPublishTaskExecutor);
    }

    //Receive incoming market data
    public void onMessage(MarketData data) {
        String symbol = data.getSymbol();
        if (!symbolUpdateRateLimiterMap.containsKey(symbol)) {
            symbolUpdateRateLimiterMap.put(symbol, RateLimiter.create(1));
        }
        if (!symbolLockBucket.containsKey(symbol)) {
            symbolLockBucket.put(symbol, symbol);
        }
        synchronized (symbolLockBucket.get(symbol)) {
            if (!symbolUpdateQueueMap.containsKey(symbol)) {
                symbolUpdateQueueMap.put(symbol, new ArrayList<MarketData>());
            }
            List<MarketData> symbolList = symbolUpdateQueueMap.get(symbol);
            symbolList.add(data);
        }
    }

    public void start() {
        //0. Thread pool auto-started when created.
        //1. Start publisher
        this.marketDataPublisher.start();
        //2. Start picker
        this.marketDataPicker.start();
    }

    public void shutdown() {
        //1. Stop picker
        this.marketDataPicker.stop();
        //2. Stop publisher
        this.marketDataPublisher.stop();
        //3. Stop thread pool
        this.marketDataPublishTaskExecutor.stop();
    }

    public void publishAggregatedMarketData(MarketData data) {

        // Do Nothing, assume implemented.

    }
}
