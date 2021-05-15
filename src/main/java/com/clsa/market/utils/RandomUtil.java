package com.clsa.market.utils;

import java.util.Random;

/**
 * @Author Bill Tu
 * @Time 2021-05-21 1:42:34
 */
public final class RandomUtil {
    private final static Random random = new Random();

    private RandomUtil() {
    }

    public static int next(int min, int max) {
        return random.nextInt(max + 1 - min) + min;
    }

}
