package com.windlike.io.reader;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashLongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import com.windlike.io.Constants;
import com.windlike.io.Platforms;
import com.windlike.io.util.InvincibleConvertUtil;
import com.windlike.io.vo.ActivityVo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by windlike.xu on 2018/3/4.
 */
public class FirstOrderFileReader implements Runnable{

    private String orignFileFullName;

    private HashIntSet userIdSet;

    private int row = 0;//行数

    private int mode = 0; // 0 - master, 1 -slave

    private HashLongObjMap<ActivityVo> activityMap = null;//仅在master模式下使用
    private AtomicInteger counter;//counter 仅在master模式下使用

    /**
     * slave模式
     * @param orignFileFullName
     * @param userIdSet
     */
    public FirstOrderFileReader(String orignFileFullName, HashIntSet userIdSet) {
        this.orignFileFullName = orignFileFullName;
        this.userIdSet = userIdSet;
        this.mode = 1;
    }

    /**
     * master模式
     * @param orignFileFullName
     * @param userIdSet
     * @param activityMap
     */
    public FirstOrderFileReader(String orignFileFullName, HashIntSet userIdSet, HashLongObjMap<ActivityVo> activityMap, AtomicInteger counter) {
        this.orignFileFullName = orignFileFullName;
        this.userIdSet = userIdSet;
        this.activityMap = activityMap;
        this.counter = counter;//当counter==0时，程序输出结果
    }

    public void run() {
        //启动读写任务
        File file  = new File(orignFileFullName);
        try (FileInputStream orginFile = new FileInputStream(file)){
            MappedByteBuffer inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());


            readAndProcess(inputBuffer);

            //清理
            userIdSet = null;
            activityMap = null;

            System.out.println("此first order任务完成，解释行数：" + row);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readAndProcess(MappedByteBuffer byteBuffer){
        byte[] fileLineCache = new byte[50];
        int length = 0;

        int segmentType = 0;
        ActivityVo activityVo = null;
        long actPlatform = 0;
        byte platform = 0;
        long actName = 0;
        int endMonDay = 0;
        while (byteBuffer.hasRemaining()){
            switch (segmentType){
                case 0://platform
                    byteBuffer.position(byteBuffer.position() + 1);//移动一位
                    platform = Platforms.shortNameToIndex((char) byteBuffer.get());
                    actPlatform += platform;
                    byteBuffer.position(byteBuffer.position() + Platforms.indexToLeftCharNum(platform));

                    break;
                case 1://actname
                    byteBuffer.get(fileLineCache, 0, 17);
//					length = getSegment(byteBuffer, fileLineCache);
                    actName = InvincibleConvertUtil.byteArrayToLong(fileLineCache, 0 ,17);//yyyyMMddHHmmssSSS
                    actPlatform += actName << 2;

                    activityVo = activityMap.get(actPlatform);
                    if(activityVo == null){//new
                        activityVo = new ActivityVo();
                        activityMap.put(actPlatform, activityVo);

                        activityVo.setActPlatfrom(actPlatform);
                        //actStartTime
                        activityVo.setStartTime(InvincibleConvertUtil.byteArrayToInt(fileLineCache, 4, 8) * 1000000 + 100000);//10点开场
                        //skip starttime
                        byteBuffer.position(byteBuffer.position() + 26);
                    }else{//old
                        byteBuffer.position(byteBuffer.position() + 51);//50=21+20+10 brandname至少10个字符
                        segmentType++;//跳过case2，直达case3
                    }

                    break;
                case 2://act_end_time
                    fileLineCache[0] = byteBuffer.get();
                    fileLineCache[1] = byteBuffer.get();
                    byteBuffer.position(byteBuffer.position() + 1);//移动一位
                    fileLineCache[2] = byteBuffer.get();
                    fileLineCache[3] = byteBuffer.get();
                    endMonDay = InvincibleConvertUtil.byteArrayToInt(fileLineCache, 0, 4);
                    activityVo.setEndTime(endMonDay * 1000000 + 95959);
                    byteBuffer.position(byteBuffer.position() + 20);//brandname最少10字符 20=10+10

                    break;
                case 3://brandId
                    getSegment(byteBuffer, fileLineCache);//空调用，忽略brand_name

                    length = getSegment(byteBuffer, fileLineCache);
                    long brandId = InvincibleConvertUtil.byteArrayToInt(fileLineCache, 0, length);
                    long brandPlatform = (brandId << 2) + platform;
                    long brandAct = (actName - 20170000000000000L) * 10000 + endMonDay;
                    HashLongSet brandActSet = brandMap.get(brandPlatform);
                    if(brandActSet != null){
                        brandActSet.add(brandAct);
                    }else{
                        brandActSet = HashLongSets.newUpdatableSet(16);
                        brandMap.put(brandPlatform, brandActSet);
                        brandActSet.add(brandAct);
                    }

                    break;
            }

            if(segmentType == 3){//一行结束，重置
                segmentType = 0;
                activityVo = null;
                actPlatform = 0;

                row++;//// TODO: 2018/3/1
            }else{
                segmentType++;
            }
        }

        System.out.println("行数：" + row);
    }

    private int getSegment(MappedByteBuffer inputBuffer, byte[] fileLineCache){
        int i = 0;
        try{
            for(; i < fileLineCache.length; i++){
                byte b = inputBuffer.get();
                if(b == Constants.SEGMENT_SEPARATE_SYMBOL || b == Constants.NEW_LINE_CHAR_ASCII){
                    break;
                }
                fileLineCache[i] = b;
            }
        }catch (BufferUnderflowException e){
            //已经是结尾
        }
        return i;
    }
}
