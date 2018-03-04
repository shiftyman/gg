package com.windlike.io;

import com.koloboke.collect.impl.hash.Hash;
import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.collect.set.hash.HashLongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import com.windlike.io.bio.Client;
import com.windlike.io.bio.Protocol;
import com.windlike.io.bio.Server;
import com.windlike.io.counter.NativeCounter;
import com.windlike.io.reader.ActFileReader;
import com.windlike.io.reader.ActFileReader2;
import com.windlike.io.reader.LikeGoodsFileReader;
import com.windlike.io.reader.LikeGoodsFileReader2;
import com.windlike.io.util.TopKComputer;
import com.windlike.io.vo.ActivityVo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by windlike.xu on 2018/3/3.
 */
public class MasterApp {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("App run in master mode");

        LinkedBlockingQueue<List<ActivityVo>> remoteTopN = new LinkedBlockingQueue<>(2);


        Server server = new Server(Constants.SERVER_LISTENING_PORT);
        server.start();

        sendStartToSlave();

        //native counting start and get native topN result
        NativeCounter nativeCounter = null;
        if(args.length > 0){
            nativeCounter = new NativeCounter(Integer.parseInt(args[0]));
        }else{
            nativeCounter = new NativeCounter();
        }
        nativeCounter.count();
        HashLongObjMap<ActivityVo> activityMap = nativeCounter.getActivityMap();
//        HashLongObjMap<HashLongSet> brandMap = nativeCounter.getBrandMap();
        nativeCounter = null;

        //get remote topN result
        mergeToMap(activityMap, remoteTopN.take());
        mergeToMap(activityMap, remoteTopN.take());

        //cal top k
        ActivityVo[] topK = TopKComputer.computeTopKAfterMerge(activityMap, Constants.K);
        activityMap = null;

        //get user first time
        HashIntSet userIdSet = getUiqueUserSet(topK);


    }

    private static HashIntSet getUiqueUserSet(ActivityVo[] activityVos){
        HashIntSet userIdSet = HashIntSets.newUpdatableSet(4000000);
        for(ActivityVo activityVo : activityVos){
            HashIntIntMap userNumMap = activityVo.getUserNumMap();
            userNumMap.keySet().cursor().forEachForward((userId)->userIdSet.add(userId));
        }
        return userIdSet;
    }

    private  static void mergeToMap(HashLongObjMap<ActivityVo> activityMap, List<ActivityVo> list){
        int delete = 0;
        for(ActivityVo vo : list){
            ActivityVo old = activityMap.get(vo.getActPlatfrom());
            if(old != null){
                old.merge(vo);
            }else{
                //舍弃
                delete ++;
            }
        }
        System.out.println("舍弃远程结果数目：" + delete);
    }

    private static void sendStartToSlave(){
        //send 'start' to slave
        boolean msgSendSuccessd = false;
        do{
            try {
                new Client(Constants.SLAVE_IP_1, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.START, null);
                msgSendSuccessd = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while (!msgSendSuccessd);

        msgSendSuccessd = false;
        do{
            try {
                new Client(Constants.SLAVE_IP_2, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.START, null);
                msgSendSuccessd = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while (!msgSendSuccessd);
    }

    private static void sendUserOrderTimeQuery(HashIntSet userIdSet){
        new Thread(()->{
            boolean msgSendSuccessd = false;
            do{
                try {
                    new Client(Constants.SLAVE_IP_1, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.QUERY_FIRST_ORDER, userIdSet);
                    msgSendSuccessd = true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } while (!msgSendSuccessd);
        }).start();

        new Thread(()->{
            boolean msgSendSuccessd = false;
            do{
                try {
                    new Client(Constants.SLAVE_IP_2, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.QUERY_FIRST_ORDER, userIdSet);
                    msgSendSuccessd = true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } while (!msgSendSuccessd);
        }).start();
    }
}
