package com.imap.flink;


/**
 * @Author: Weizhi
 * @Date: create in 2023/2/27 21:19
 * @Description:
 */
public enum AvgDataEnum {
    MINUTE(1, 1),
    HOUR(2, 60),
    DAY(3, 60 * 24),
    WEEK(4, 60 * 24 * 7),
    MONTH(5, 60 * 24 * 30);

    private final int type;
    private final int minutes;

    AvgDataEnum(int type, int minutes) {
        this.type = type;
        this.minutes = minutes;
    }

    public static AvgDataEnum from(int type) {
        switch (type) {
            case 1:
                return MINUTE;
            case 2:
                return HOUR;
            case 3:
                return DAY;
            case 4:
                return WEEK;
            case 5:
                return MONTH;
        }
        return MONTH;
    }

    public int getType() {
        return type;
    }

    public int getMinutes() {
        return minutes;
    }

}
