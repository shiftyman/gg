package com.windlike.io.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by windlike.xu on 2018/2/14.
 */
public class InvincibleConvertUtil {

    public static long stringToLong(String str){
        long sum = 0;
        for(int i = 0; i < str.length(); i ++){
            sum = sum * 10 + ((byte) str.charAt(i) - 48);
        }
        return sum;
    }

    public static int stringToInt(String str){
        int sum = 0;
        for(int i = 0; i < str.length(); i ++){
            sum = sum * 10 + ((byte) str.charAt(i) - 48);
        }
        return sum;
    }

    public static long byteArrayToLong(byte[] bytes, int start, int length){
        long sum = 0;
        for(int i = start; i < length; i ++){
            sum = sum * 10 + (bytes[i] - 48);
        }
        return sum;
    }

    public static int byteArrayToInt(byte[] bytes, int start, int length){
        int sum = 0;
        for(int i = start; i < length; i ++){
            sum = sum * 10 + (bytes[i] - 48);
        }
        return sum;
    }

    public static byte stringToByte(String str){
        byte sum = 0;
        for(int i = 0; i < str.length(); i ++){
            sum = (byte) (sum * 10 + ((byte) str.charAt(i) - 48));
        }
        return sum;
    }

    public static List<String> split(String str, char separatorChar, int expectParts) {
        if(str == null) {
            return null;
        } else {
            int len = str.length();
            if(len == 0) {
                return null;//new String[0];
            } else {
                ArrayList<String> list = new ArrayList(expectParts);
                int i = 0;
                int start = 0;
                boolean match = false;

                while(i < len) {
                    if(str.charAt(i) == separatorChar) {
                        if(match) {
                            list.add(str.substring(start, i));
                            match = false;
                        }

                        ++i;
                        start = i;
                    } else {
                        match = true;
                        ++i;
                    }
                }

                if(match) {
                    list.add(str.substring(start, i));
                }

//                return list.toArray(new String[list.size()]);
                return list;
            }
        }
    }

//    private int actNameToStartTime(long actName){
//
//    }

    public static void main(String[] args) {
        String str = "sdfdsf sdfds sdf xvcc sdfsd werew";

        long t2 = System.currentTimeMillis();

        for (int j = 0; j < 100000000; j++) {
//            int i = charToNum(c);
            split(str, ' ', 10);
        }

        long t3 = System.currentTimeMillis();

        System.out.println((t3-t2));

    }


}
