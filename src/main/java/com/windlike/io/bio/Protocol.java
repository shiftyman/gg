package com.windlike.io.bio;

/**
 * 协议：header（msgtype.ordinal）   body（根据各type的协议）
 * Created by windlike.xu on 2018/2/26.
 */
public class Protocol {

    public enum MsgType{
        /**
         * empty msg
         */
        START,

        /**
         * 协议：活动数量short、活动名平台long、品牌名string、收藏总数int、开始时间int、结束时间int、
         * 独立用户数量int、userintems（userid-int、此user个数int）
         */
        REPORT_TOPK,


        QUERY_MORE,


        RESPONSE_MORE,


        QUERY_FIRST_ORDER,

        RESPONSE_FIRST_ORDER,

        /**
         * empty msg
         */
        END;
    }

}
