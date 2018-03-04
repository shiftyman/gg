package com.windlike.io.vo;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.windlike.io.Constants;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class ActivityVo implements Comparable<ActivityVo>, Cloneable{

    private long actPlatfrom;//yyyyMMddHHmmssSSS|pFlag pFlag占后两位

    private int startTime;//timestamp

    private int endTime;

    private int allNum;

    private HashIntIntMap userNumMap;

//    private HashIntSet brandIdSet;

    private boolean hasSent = false;

    private byte mergeNum = 1;

    public ActivityVo() {
        userNumMap = HashIntIntMaps.newUpdatableMap(7500);
//        brandIdSet = HashIntSets.newUpdatableSet(16);
    }

    /**
     *
     * @param actPlatfrom
     * @param startTime
     * @param endTime
     * @param allNum
     * @param userNum
     */
    public ActivityVo(long actPlatfrom, int startTime, int endTime, int allNum, int userNum) {
        this.actPlatfrom = actPlatfrom;
        this.startTime = startTime;
        this.endTime = endTime;
        this.allNum = allNum;
        this.userNumMap = HashIntIntMaps.newUpdatableMap(userNum);
//        this.brandIdSet = HashIntSets.newUpdatableSet(brandIdNum);
    }

    public ActivityVo(ActivityVo activityVo) {
        this.actPlatfrom = activityVo.getActPlatfrom();
        this.startTime = activityVo.getStartTime();
        this.endTime = activityVo.getEndTime();
        this.allNum = activityVo.getAllNum();
        this.userNumMap = activityVo.getUserNumMap();
//        this.brandIdSet = activityVo.getBrandIdSet();
    }

    public void merge(ActivityVo vo){
        this.allNum += vo.getAllNum();
        userNumMap.cursor().forEachForward((k, v)->this.userNumMap.addValue(k, v));
        mergeNum ++;
    }

//    public HashIntSet getBrandIdSet() {
//        return brandIdSet;
//    }

//    public void setBrandIdSet(HashIntSet brandIdSet) {
//        this.brandIdSet = brandIdSet;
//    }

    public synchronized void addUser(int userId, int num){
        userNumMap.addValue(userId, num);
        allNum += num;
    }

//    public void addBrandId(int brandId){
//        brandIdSet.add(brandId);
//    }

    public void setHasSent(boolean hasSent) {
        this.hasSent = hasSent;
    }

    @Override
    public String toString() {
        return "Brand{" +
                "actPlatfrom='" + actPlatfrom + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", allNum=" + allNum +
                ", userNum=" + userNumMap.size() +
                '}';
    }

    public void setActPlatfrom(long actPlatfrom) {
        this.actPlatfrom = actPlatfrom;
    }

    public void setStartTime(int startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(int endTime) {
        this.endTime = endTime;
    }

    public void setUserNumMap(HashIntIntMap userNumMap) {
        this.userNumMap = userNumMap;
    }

    public void setAllNum(int allNum) {
        this.allNum = allNum;
    }

    public void setMergeNum(byte mergeNum) {
        this.mergeNum = mergeNum;
    }

    public long getActPlatfrom() {
        return actPlatfrom;
    }

    public int getStartTime() {
        return startTime;
    }

    public int getEndTime() {
        return endTime;
    }

    public HashIntIntMap getUserNumMap() {
        return userNumMap;
    }

    public int getAllNum() {
        return allNum;
    }

    public boolean isHasSent() {
        return hasSent;
    }

    public byte getMergeNum() {
        return mergeNum;
    }

    public int compareByAllNumAsc(ActivityVo o) {
        if(this.allNum > o.allNum){
            return -1;
        }else if(this.allNum < o.allNum){
            return 1;
        }else {
            return 0;
        }
    }

    public int compareByAllNumDsc(ActivityVo o) {
        if(this.allNum > o.allNum){
            return 1;
        }else if(this.allNum < o.allNum){
            return -1;
        }else {
            return 0;
        }
    }

    public int compareFullAsc(ActivityVo o) {
        if(this.allNum > o.allNum){
            return -1;
        }else if(this.allNum < o.allNum){
            return 1;
        }else {
            if(this.userNumMap.size() > o.userNumMap.size()){
                return -1;
            }else if(this.userNumMap.size() < o.userNumMap.size()){
                return 1;
            }else{
                if(this.actPlatfrom < o.actPlatfrom){
                    return -1;
                }else{
                    return 1;
                }
            }
        }
    }

    @Override
    public int compareTo(ActivityVo o) {
        return compareByAllNumDsc(o);
    }

    public static void main(String[] args) {

//        HashLongLongMap longLongMap = HashLongLongMaps.newUpdatableMap();
//        longLongMap.addValue(1, 3);
//        longLongMap.addValue(2, 4);
//        System.out.println(longLongMap.get(1));
//        longLongMap.cursor().forEachForward((k, v)-> System.out.println(k + "," + v));

        byte s1 = (byte) (0xf0 + 1);
        byte s2 = (byte) (0xf0 + 2);
        System.out.println(s1&s2);
        System.out.println((byte)0xf0);

        System.out.println(Constants.NOT_SENT_FLAG);
        System.out.println(Constants.SENT_FLAG);
    }

    //    }
//        return mflag;

}
