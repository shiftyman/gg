package com.windlike.io;

/**
 * Created by windlike.xu on 2018/2/28.
 */
public class Constants {

    public static final byte SENT_FLAG = (byte) 0xf0;

    public static final byte NOT_SENT_FLAG = (byte) 0x00;

    public static final int DEFAULT_ACT_SIZE = 20000;

    /**
     * 换行符ascii
     */
    public final static int NEW_LINE_CHAR_ASCII = 10;

    public final static int SEGMENT_SEPARATE_SYMBOL = 44;

    public final static int EOF_FILE = -1;

    public final static int VALID_YEAR_ASCII = 55;//7

    public final static String ACT_DATA_FILE = "/data2/dty_act_warmup_brand_total_hm/0-0.txt";

    public final static String LIKE_GODDS_DATA_FILE_PATH = "/data2/trd_brand_goods_like_hm/";

    public final static int VALID_CANDIDATE_NUM = 200;

    public static void main(String[] args) {
        long actPlatform = 80681688899415834L;
        System.out.println(Platforms.indexToFullName((byte) (actPlatform & 3)));
        System.out.println(actPlatform >> 2);
    }
}
