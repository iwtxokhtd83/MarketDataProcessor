package com.clsa.market.app;

import com.clsa.market.model.MarketData;
import com.clsa.market.service.MarketDataProcessor;
import com.clsa.market.utils.RandomUtil;
import io.github.benas.randombeans.EnhancedRandomBuilder;
import io.github.benas.randombeans.api.EnhancedRandom;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author Bill Tu
 * @Time 2021-05-15 1:23:46
 * Assumptions:
 * 1. The rate(100 times/second) is for single node instead of a cluster.
 * 2. Check if a symbol is updated by using updatedTs.
 * 3. Only latest market data need to be published.
 */
@Slf4j
public class MarketDataApplication {
    private final String[] sampleSymbols = new String[]{"0001.HK", "0002.HK", "0003.HK", "0004.HK", "0005.HK", "0006.HK", "0007.HK",
            "0008.HK", "0009.HK", "0010.HK", "0011.HK", "0012.HK", "0013.HK", "0014.HK", "0015.HK", "0016.HK", "0017.HK", "0018.HK",
            "0019.HK", "0020.HK", "0021.HK", "0022.HK", "0023.HK", "0024.HK", "0025.HK"};
    private final EnhancedRandom enhancedRandom = EnhancedRandomBuilder.aNewEnhancedRandom();
    private MarketDataProcessor marketDataProcessor = new MarketDataProcessor();
    //Single thread to call onMessage()
    private final Thread marketDataGenerator = new Thread(new Runnable() {
        public void run() {
            for (int i = 0; i < 10000; i++) {
                MarketData marketData = enhancedRandom.nextObject(MarketData.class);
                marketData.setSymbol(sampleSymbols[RandomUtil.next(0, sampleSymbols.length - 1)]);
                marketDataProcessor.onMessage(marketData);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    });

    public static void main(String[] args) {
        MarketDataApplication application = new MarketDataApplication();
        //Start market data processor
        application.marketDataProcessor.start();
        //Generate market data for testing.
        application.marketDataGenerator.start();
    }
}
