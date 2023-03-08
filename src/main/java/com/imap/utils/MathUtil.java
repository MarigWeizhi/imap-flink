package com.imap.utils;

import java.math.BigDecimal;
import java.util.Random;

/**
 * @Author: Weizhi
 * @Date: create in 2023/3/8 0:34
 * @Description:
 */
public class MathUtil {

    public static Double getRandomData(int n, int scale) {
        Random random = new Random();
        double base = Math.pow(10,n);
        BigDecimal bigDecimal = new BigDecimal(random.nextDouble() * base)
                .setScale(scale, BigDecimal.ROUND_HALF_UP);
        return bigDecimal.doubleValue();
    }
}
