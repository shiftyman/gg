package com.windlike.io.bio;

import com.windlike.io.vo.BrandTransferVo;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.net.Socket;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class Worker implements Runnable{

    private Socket socket;

    public Worker(Socket socket){
        this.socket = socket;
    }

    private BrandTransferVo[] brands;

    @Override
    public void run() {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()))) {

            //协议：品牌数量short、品牌名size-short、品牌名string、总数int、开始时间long、结束时间long、品牌id-long、
            // 平台byte、独立用户数量int、userintems（userid-long、此user个数short）
            short brandSize = input.readShort();
            byte[] brandNameBytes = new byte[512];
            brands = new BrandTransferVo[brandSize];
            for(int i = 0; i < brandSize; i++){
                short brandNameSize = input.readShort();
                input.readFully(brandNameBytes, 0, brandNameSize);
                String brandName = new String(brandNameBytes, 0, brandNameSize);
                int allNum = input.readInt();
                long startTime = input.readLong();
                long endTime = input.readLong();
                long brandId = input.readLong();
                byte platform = input.readByte();
                int userNum = input.readInt();
                BrandTransferVo brand = new BrandTransferVo(brandName,brandId,platform,startTime,endTime,allNum
                    ,userNum);
                for (int j = 0; j < userNum; j++) {
                    long userId = input.readLong();
                    short num = input.readShort();
                    brand.addUser(userId, num);
                }
                brands[i] = brand;

                Thread.sleep(2000L);
            }

            printBrands();//打印看看？

        } catch (Exception e) {
            e.printStackTrace();
            //意外，需要抛弃此次处理的数据，等待客户端再次连接并发送
        }finally {
            try {
                if(socket != null) socket.close();
                System.out.println("server socket closed.");
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    private void printBrands(){
        for(BrandTransferVo brand : brands){
            System.out.println(brand);
        }
    }
}
