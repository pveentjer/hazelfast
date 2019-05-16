package com.hazelfast;

import com.hazelfast.impl.DataStructures;
import com.hazelfast.impl.IOUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.hazelfast.impl.IOUtil.compactOrClear;

public class Client {

    private InetSocketAddress address;
    private SocketChannel socketChannel;
    ByteBuffer sendBuffer;
    private ByteBuffer receiveBuffer;
    private final String hostname;
    private final int receiveBufferSize;
    private final int sendBufferSize;
    private final boolean tcpNoDelay;
    private final boolean directBuffers;
    private final boolean objectPoolingEnabled;
    private final Counters counters;
    private final Strings strings;

    public Client(Context context) {
        hostname = context.hostname;
        receiveBufferSize = context.receiveBufferSize;
        sendBufferSize = context.sendBufferSize;
        tcpNoDelay = context.tcpNoDelay;
        directBuffers = context.directBuffers;
        objectPoolingEnabled = context.objectPoolingEnabled;
        counters = new Counters(this);
        strings = new Strings(this);
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
        Client client1 = new Client(new Context());
        Client client2 = new Client(new Context());
        client1.start();
        client2.start();

        //client.dummyLoop();
        client1.stop();
        client2.stop();
    }

    public void start() throws IOException {
        log("Connecting to Server on startPort 1111...");

        this.address = new InetSocketAddress(hostname, 1111);
        sendBuffer = directBuffers
                ? ByteBuffer.allocateDirect(receiveBufferSize)
                : ByteBuffer.allocate(receiveBufferSize);
        receiveBuffer = directBuffers
                ? ByteBuffer.allocateDirect(sendBufferSize)
                : ByteBuffer.allocate(sendBufferSize);

        socketChannel = SocketChannel.open(address);
        socketChannel.socket().setTcpNoDelay(tcpNoDelay);
        socketChannel.socket().setReceiveBufferSize(receiveBufferSize);
        socketChannel.socket().setSendBufferSize(sendBufferSize);
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
                System.out.println("Writing request");
                ping(new byte[100 * 1024]);
                System.out.println("Reading response");
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

    public void dummyLoopInternal() throws IOException {

        try {
            long startMs = System.currentTimeMillis();
            int count = 100000;
            for (int k = 0; k < count; k++) {
                System.out.println("Writing request");
                ping(new byte[100 * 1024]);
                System.out.println("Reading response");
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

    public void ping(String message) throws IOException {
        ping(message.getBytes());
    }

    public void ping(byte[] message) throws IOException {
        sendBuffer.putInt(message.length+1);
        //sendBuf.putInt(1);
        sendBuffer.put(DataStructures.PING);
        sendBuffer.put(message);

        sendBuffer.flip();
        int written = socketChannel.write(sendBuffer);
        //System.out.println("send "+written+" bytes, remaining:"+sendBuf.remaining());


        //log("sending: " + companyName);
        sendBuffer.clear();
    }

    protected void write() {
        try {
            sendBuffer.flip();
            socketChannel.write(sendBuffer);

            //log("sending: " + companyName);
            sendBuffer.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] pooledBytes;

    public void readResponse() throws IOException {
        byte[] bytes = null;

        int offset = 0;
        for (; ; ) {
            // System.out.println(Util.toDebugString("receiveBuf",receiveBuf));
            int read = socketChannel.read(receiveBuffer);

            if (read == -1) {
                socketChannel.close();
                throw new IOException("Socket Closed by remote");
            }

            //System.out.println("read:" + read+" bytes,"+Util.toDebugString("receiveBuf",receiveBuf));
            receiveBuffer.flip();

            try {
                if (bytes == null) {
                    if (receiveBuffer.remaining() < IOUtil.INT_AS_BYTES) {
                        continue;
                    }

                    int len = receiveBuffer.getInt();
                    if (objectPoolingEnabled && pooledBytes != null && pooledBytes.length >= len) {
                        bytes = pooledBytes;
                        pooledBytes = null;
                    } else {
                        bytes = new byte[len];
                    }
                }
                int needed = bytes.length - offset;
                int length;
                boolean complete = false;
                if (receiveBuffer.remaining() >= needed) {
                    length = needed;
                    complete = true;
                } else {
                    length = receiveBuffer.remaining();
                }

                receiveBuffer.get(bytes, offset, length);
                if (complete) {
                    if (objectPoolingEnabled) {
                        pooledBytes = bytes;
                    }
                    return;

                } else {
                    offset += length;
                }
            } finally {
                compactOrClear(receiveBuffer);
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