package com.clsa.market.model;

import lombok.*;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @Author Bill Tu
 * @Time 2021-05-15 10:01:43
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MarketData implements Comparable<MarketData> {

    private String symbol;

    private BigDecimal bid;

    private BigDecimal ask;

    private BigDecimal last;

    private Date updateTs;


    public int compareTo(MarketData o) {
        if (this.getUpdateTs().getTime() > o.getUpdateTs().getTime()) {
            return 1;
        } else if (this.getUpdateTs().getTime() == o.getUpdateTs().getTime()) {
            return 0;
        } else {
            return -1;
        }
    }
}
