package com.hazelfast;

import com.hazelfast.impl.ByteArrayPool;
import com.hazelfast.impl.DataStructures;
import com.hazelfast.impl.Frame;
import com.hazelfast.impl.FramePool;
import com.hazelfast.impl.IOUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.hazelfast.impl.IOUtil.INT_AS_BYTES;
import static com.hazelfast.impl.IOUtil.compactOrClear;
import static com.hazelfast.impl.IOUtil.setReceiveBufferSize;
import static com.hazelfast.impl.IOUtil.setSendBufferSize;

public class Client {

    private final FramePool framePool;
    private InetSocketAddress address;
    private SocketChannel socketChannel;
    ByteBuffer sendBuf;
    private ByteBuffer receiveBuf;
    private final String hostname;
    private final int receiveBufferSize;
    private final int sendBufferSize;
    private final boolean tcpNoDelay;
    private final boolean directBuffers;
    private final boolean objectPoolingEnabled;
    private final Counters counters;
    private final Strings strings;
    private final ByteArrayPool byteArrayPool;

    public Client(Context context) {
        hostname = context.hostname;
        receiveBufferSize = context.receiveBufferSize;
        sendBufferSize = context.sendBufferSize;
        tcpNoDelay = context.tcpNoDelay;
        directBuffers = context.directBuffers;
        objectPoolingEnabled = context.objectPoolingEnabled;
        counters = new Counters(this);
        strings = new Strings(this);
        this.byteArrayPool = new ByteArrayPool(objectPoolingEnabled);
        this.framePool  = new FramePool(objectPoolingEnabled);
    }

    public String hostname() {
        return hostname;
    }

    public int receiveBufferSize() {
        return receiveBufferSize;
    }

    public int sendBufferSize() {
        return sendBufferSize;
    }

    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    public boolean directBuffers() {
        return directBuffers;
    }

    public boolean objectPoolingEnabled() {
        return objectPoolingEnabled;
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client(new Context());
        client.start();

        client.dummyLoop();
        client.stop();
    }

    public void start() throws IOException {
        log("Connecting to Server on startPort 1111...");

        this.address = new InetSocketAddress(hostname, 1111);
        sendBuf = directBuffers
                ? ByteBuffer.allocateDirect(receiveBufferSize)
                : ByteBuffer.allocate(receiveBufferSize);
        receiveBuf = directBuffers
                ? ByteBuffer.allocateDirect(sendBufferSize)
                : ByteBuffer.allocate(sendBufferSize);

        socketChannel = SocketChannel.open(address);
        socketChannel.socket().setTcpNoDelay(tcpNoDelay);
        setReceiveBufferSize(socketChannel, receiveBufferSize);
        setSendBufferSize(socketChannel, sendBufferSize);
    }

    public Counters counters() {
        return counters;
    }

    public void stop() throws IOException {
        socketChannel.close();
    }

    public void dummyLoop() throws IOException {
        try {
            long startMs = System.currentTimeMillis();
            int count = 100000;
            for (int k = 0; k < count; k++) {
                System.out.println("Writing request1");
                writeAndFlush(new byte[100]);
                System.out.println("Writing request1");
                writeAndFlush(new byte[100]);
                System.out.println("Reading response1");
                readResponse();
                System.out.println("Reading response2");
                readResponse();
                System.out.println("k:" + k);
            }
            long durationMs = System.currentTimeMillis() - startMs;

            System.out.println("Duration:" + durationMs + " ms");
            System.out.println("Throughput:" + (1000f * count) / durationMs + " msg/second");
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


    public void writeAndFlush(String message) throws IOException {
        writeAndFlush(message.getBytes());
    }

    public void writeAndFlush(byte[] message) throws IOException {
        write(message);
        flush();
    }

    public void flush() throws IOException {
        sendBuf.flip();
        int written = socketChannel.write(sendBuf);
//        System.out.println("send " + written + " bytes");

        //log("sending: " + companyName);
        sendBuf.clear();
    }

    public void write(byte[] message) {
        sendBuf.putInt(message.length + 1);
        //sendBuf.putInt(1);
        sendBuf.put(DataStructures.PING);
        sendBuf.put(message);
    }

    protected void writeAndFlush() {
        try {
            sendBuf.flip();
            socketChannel.write(sendBuf);

            //log("sending: " + companyName);
            sendBuf.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void readResponse() throws IOException {
        Frame frame = null;
        int offset = 0;

        boolean skipRead = true;
        for (; ; ) {
            if(!skipRead) {
                int read = socketChannel.read(receiveBuf);
                if (read == -1) {
                    socketChannel.close();
                    throw new IOException("Socket Closed by remote");
                }
             //   System.out.println("read:" + read + " bytes");
            }

            skipRead = false;

            receiveBuf.flip();
            try {
                if (frame == null) {
                    if (receiveBuf.remaining() < INT_AS_BYTES) {
                        continue;
                    }

                    frame = framePool.takeFromPool();
                    frame.length = receiveBuf.getInt();
                    frame.bytes = byteArrayPool.takeFromPool(frame.length);
                }
                int needed = frame.length - offset;
                int length;
                boolean complete = false;
                if (receiveBuf.remaining() >= needed) {
                    length = needed;
                    complete = true;
                } else {
                    length = receiveBuf.remaining();
                }

                receiveBuf.get(frame.bytes, offset, length);
                if (complete) {
                    byteArrayPool.returnToPool(frame.bytes);
                    framePool.returnToPool(frame);
                    return;
                } else {
                    offset += length;
                }
            } finally {
                compactOrClear(receiveBuf);
            }
        }
    }

    private static void log(String str) {
        System.out.println(str);
    }

    public static class Context {
        private String hostname = "localhost";
        private int receiveBufferSize = 128 * 1024;
        private int sendBufferSize = 128 * 1024;
        private boolean tcpNoDelay = true;
        private boolean directBuffers = true;
        private boolean objectPoolingEnabled = true;

        public Context hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Context receiveBufferSize(int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        public Context sendBufferSize(int sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
            return this;
        }

        public Context tcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        public Context directBuffers(boolean directBuffers) {
            this.directBuffers = directBuffers;
            return this;
        }

        public Context objectPoolingEnabled(boolean objectPoolingEnabled) {
            this.objectPoolingEnabled = objectPoolingEnabled;
            return this;
        }
    }
}