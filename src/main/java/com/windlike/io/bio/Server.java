package com.windlike.io.bio;

import com.windlike.io.nio.ServerWorker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class Server {

    private ServerSocket serverSocket;
    private ExecutorService workers = Executors.newCachedThreadPool();

    public Server(int port){
        try {
            serverSocket  = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start(){
        new Thread(new Runnable() {
            @Override
            public void run() {

                while(true) {
                    try {
                        Socket socket = serverSocket.accept();
                        System.out.println("收到一个连接");
                        workers.execute(new Worker(socket));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        },"server-listening-thread").start();
    }

    public static void main(String[] args) {
        new Server(8080).start();
//        while (true){
            try {
                Thread.sleep(20000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//        }
    }
}
