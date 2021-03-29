package com.github.dolly0526;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * @author yusenyang
 * @create 2021/3/19 19:20
 */
public class TestZeroCopy {

    @Test
    public void testMmap() throws Exception {

        FileChannel fileChannel = new FileInputStream("/Users/sgcx017/Downloads/20210319103051.jpg").getChannel();

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(new InetSocketAddress("localhost", 8080));

        MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        socketChannel.write(byteBuffer);
    }

    @Test
    public void testServerSocket() throws Exception {

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(8080));
        SocketChannel socketChannel = serverSocketChannel.accept();

        FileChannel fileChannel = new FileOutputStream("/Users/sgcx017/Downloads/20210319103052.jpg").getChannel();

        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        int read = socketChannel.read(byteBuffer);

        while (read != -1) {

            byteBuffer.flip();
            fileChannel.write(byteBuffer);
            byteBuffer.clear();
            read = socketChannel.read(byteBuffer);
        }
    }
}
