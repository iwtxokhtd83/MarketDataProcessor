package com.clsa.market.worker;

import com.clsa.market.model.MarketData;
import com.clsa.market.service.MarketDataProcessor;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author Bill Tu
 * @Time 2021-05-15 12:22:06
 * Real action to publish aggregated market data
 */
@Slf4j
public class MarketDataPublishTask implements Runnable {
    private MarketDataProcessor marketDataProcessor;
    private MarketData latestMarketData;

    public MarketDataPublishTask(MarketDataProcessor marketDataProcessor,
                                 MarketData latestMarketData) {
        this.marketDataProcessor = marketDataProcessor;
        this.latestMarketData = latestMarketData;
    }

    public void run() {
        try {
            this.marketDataProcessor.publishAggregatedMarketData(latestMarketData);
            log.info("Published aggregated market data {}.", latestMarketData.getSymbol());
        } catch (Exception e) {
            log.error("Failed to publish latest market data [{}].", latestMarketData, e);
        }

    }

    @Override
    public String toString() {
        return "[Symbol=" + latestMarketData.getSymbol() + ",bid=" + latestMarketData.getBid() + "]";
    }
}
