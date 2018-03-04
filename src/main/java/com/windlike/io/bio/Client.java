package com.windlike.io.bio;

import com.windlike.io.vo.BrandTransferVo;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class Client {

    private Socket socket;

    private List<BrandTransferVo> brandList = new LinkedList<>();

    public Client(String host, int port){
        try {
            socket = new Socket(host, port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        init();
    }

    public void init(){
        for (int i = 0; i < 4; i++) {
            int userNum = i + 1;
            BrandTransferVo brand = new BrandTransferVo("brand" + i, i, (byte)0, 1200, 2000,
                    20, userNum);
            for (int j = 0; j < userNum; j++) {
                brand.addUser(j, j);
            }
            brandList.add(brand);
        }
    }

    public void start() throws IOException, InterruptedException {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            //协议：品牌数量short、品牌名size-short、品牌名string、总数int、开始时间long、结束时间long、品牌id-long、
            // 平台byte、独立用户数量int、userintems（userid-long、此user个数short）
            output.writeShort(brandList.size());
            for (BrandTransferVo brand : brandList){
                byte[] bytes = brand.getBrandName().getBytes();
                output.writeShort(bytes.length);
                output.write(bytes);
                output.writeInt(brand.getAllNum());
                output.writeLong(brand.getStartTime());
                output.writeLong(brand.getEndTime());
                output.writeLong(brand.getBrandId());
                output.writeByte(brand.getPlatform());

//                long[] userInfo = brand.getUserIdAndNum();
//                output.writeInt(userInfo.length >> 1);
//                for (int i = 0 ; i < userInfo.length; i=i+2){
//                    output.writeLong(userInfo[i]);
//                    output.writeShort((int) userInfo[i+1]);
//                }

//                output.flush();
//                break;
//                Thread.sleep(5000L);
            }
            output.flush();

            System.out.println("发送完毕");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if(socket != null) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            new Client("127.0.0.1", 8080).start();
        } catch (Exception e) {
            e.printStackTrace();

            //重试1次
            try {
                new Client("127.0.0.1", 8080).start();
            } catch (IOException e1) {
                e1.printStackTrace();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }
}
