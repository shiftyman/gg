package com.windlike.io.util;

import com.windlike.io.Constants;

/**
 * Created by windlike.xu on 2018/3/1.
 */
public class TestFileRead {

    public static void main(String[] args) throws InterruptedException {

        int i = 1;
        while (true){
            Thread.sleep(5000L);
            i++;
            if(i > 2){
                break;
            }
        }

        long t1 = System.currentTimeMillis();
        ActivityFileReader activityFileReader = new ActivityFileReader(Constants.ACT_DATA_FILE);
        activityFileReader.call();

        long t2 = System.currentTimeMillis();

        ActivityFileReader2 activityFileReader2 = new ActivityFileReader2(Constants.ACT_DATA_FILE);
        activityFileReader2.call();

        long t3 = System.currentTimeMillis();

        System.out.println("1:" + (t2-t1) + ",2:" + (t3-t2));

    }
}
