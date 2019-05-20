package com.hazelfast;

import com.hazelfast.impl.ByteArrayPool;
import com.hazelfast.impl.DataStructures;
import com.hazelfast.impl.Frame;
import com.hazelfast.impl.FramePool;
import com.hazelfast.impl.IOUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelfast.impl.IOUtil.INT_AS_BYTES;
import static com.hazelfast.impl.IOUtil.allocateByteBuffer;
import static com.hazelfast.impl.IOUtil.compactOrClear;
import static com.hazelfast.impl.IOUtil.setReceiveBufferSize;
import static com.hazelfast.impl.IOUtil.setSendBufferSize;
import static java.lang.Math.max;

/**
 * Ways to connect:
 * - each server thread has its own IO port.
 * - then the client needs to know how many IO threads there are per server.
 * This will not increase latency since connecting can be done in parallel
 * This requires more configuration. Server can't decide without telling the client
 * how many IO threads there are.
 * - shared io port for all server threads; but how does a client then identify the IO thread it wants to register to?
 * - then the server could tell to the client the number of remaining connections are needed.
 * This will increase latency since multiple round trips are needed.
 * This will require less configuration. Server will client how to complete the handshake.
 */
public class Server {

    private final AtomicInteger ioThreadId = new AtomicInteger(0);
    private ServerSocketChannel serverSocket;
    private InetSocketAddress serverAddress;
    private ServerThread[] serverThreads;
    private AcceptThread acceptThread;
    private final AtomicInteger nextIOThreadId = new AtomicInteger();
    private volatile boolean stopping = false;
    private final int serverThreadCount;
    private final String bindAddress;
    private final int port;
    private final int receiveBufferSize;
    private final int sendBufferSize;
    private final boolean tcpNoDelay;
    private final boolean objectPoolingEnabled;
    private final boolean optimizeSelector;
    private final boolean directBuffers;
    private final boolean selectorSpin;

    public Server(Context context) {
        this.serverThreadCount = context.serverThreadCount;
        this.bindAddress = context.bindAddress;
        this.port = context.startPort;
        this.receiveBufferSize = context.receiveBufferSize;
        this.sendBufferSize = context.sendBufferSize;
        this.tcpNoDelay = context.tcpNoDelay;
        this.objectPoolingEnabled = context.objectPoolingEnabled;
        this.optimizeSelector = context.optimizeSelector;
        this.directBuffers = context.directBuffers;
        this.selectorSpin = context.selectorSpin;
    }

    public int ioThreadCount() {
        return serverThreadCount;
    }

    public String hostname() {
        return bindAddress;
    }

    public int port() {
        return port;
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

    public boolean objectPoolingEnabled() {
        return objectPoolingEnabled;
    }

    public boolean optimizeSelector() {
        return optimizeSelector;
    }

    public boolean directBuffers() {
        return directBuffers;
    }

    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception {
        Server server = new Server(new Context().serverThreadCount(1));
//                .receiveBufferSize(1024)
//                .sendBufferSize(1024))

        server.start();
    }

    public void stop() throws IOException {
        stopping = true;
        serverSocket.close();
        acceptThread.shutdown();
        for (ServerThread serverThread : serverThreads) {
            serverThread.shutdown();
        }
    }

    public void start() throws IOException {
        serverSocket = ServerSocketChannel.open();
        serverAddress = new InetSocketAddress(bindAddress, port);
        serverSocket.bind(serverAddress);

        serverSocket.socket().setReceiveBufferSize(receiveBufferSize);
        if (serverSocket.socket().getReceiveBufferSize() != receiveBufferSize) {
            System.out.println("socket doesn't have expected receiveBufferSize, expected:"
                    + receiveBufferSize + " actual:" + serverSocket.socket().getReceiveBufferSize());
        }

        serverSocket.configureBlocking(false);

        this.serverThreads = new ServerThread[serverThreadCount];
        for (int k = 0; k < serverThreadCount; k++) {
            serverThreads[k] = new ServerThread();
            serverThreads[k].start();
        }
        this.acceptThread = new AcceptThread();
        acceptThread.start();
    }

    private ServerThread nextIOThread() {
        int next = nextIOThreadId.getAndIncrement() % serverThreadCount;
        return serverThreads[next];
    }

    private class ServerThread extends Thread {
        private final Selector selector;
        private final ConcurrentLinkedQueue<SocketChannel> newChannels = new ConcurrentLinkedQueue<>();
        private final DataStructures ds = new DataStructures();

        private ServerThread() throws IOException {
            super("IOThread#" + ioThreadId.getAndIncrement());
            setDaemon(true);
            selector = optimizeSelector ? IOUtil.newSelector() : Selector.open();
        }

        @Override
        public void run() {
            log(getName() + " running");
            try {
                selectLoop();
            } catch (Exception e) {
                if (!stopping) {
                    log(e);
                }
            }
        }

        private void selectLoop() throws IOException {
            for (; ; ) {
                int selectedKeys = selectorSpin ? selector.selectNow() : selector.select();
                registerNewChannels();
                if (selectedKeys == 0) continue;

                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey sk = it.next();
                    it.remove();

                    try {
                        if (sk.isReadable()) onRead(sk);
                        if (sk.isWritable()) onWrite(sk);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        sk.channel().close();
                    }
                }
            }
        }

        private void registerNewChannels() throws IOException {
            for (; ; ) {
                SocketChannel channel = newChannels.poll();
                if (channel == null) break;

                channel.configureBlocking(false);
                channel.socket().setTcpNoDelay(tcpNoDelay);
                Connection con = new Connection(objectPoolingEnabled);
                con.receiveBuf = allocateByteBuffer(directBuffers, receiveBufferSize);
                setReceiveBufferSize(channel, receiveBufferSize);
                con.sendBuf = allocateByteBuffer(directBuffers, sendBufferSize);
                setSendBufferSize(channel, sendBufferSize);
                con.channel = channel;
                channel.register(selector, SelectionKey.OP_READ, con);
            }
        }

        private void onWrite(SelectionKey sk) throws IOException {
            //System.out.println("onWrite");
            SocketChannel channel = (SocketChannel) sk.channel();
            Connection con = (Connection) sk.attachment();
            // todo: this field is increased even if we are triggered from the onRead
            con.onWriteEvents++;

            for (; ; ) {
                if (con.sendFrame == null) {
                    // check if there is enough space to writeAndFlush the length
                    if (con.sendBuf.remaining() < INT_AS_BYTES) break;

                    con.sendFrame = con.pending.poll();
                    if (con.sendFrame == null) break;

                    //System.out.println("onWrite:" + con.sendFrame);

                    con.sendBuf.putInt(con.sendFrame.length);
                }

                int missingFromFrame = con.sendFrame.length - con.sendOffset;
                int bytesToWrite;
                boolean complete;
                if (con.sendBuf.remaining() <= missingFromFrame) {
                    bytesToWrite = con.sendBuf.remaining();
                    complete = false;
                } else {
                    bytesToWrite = missingFromFrame;
                    complete = true;
                }

                con.sendBuf.put(con.sendFrame.bytes, con.sendOffset, bytesToWrite);

                if (complete) {
                    //System.out.println("send frame complete:" + con.sendFrame);
                    con.byteArrayPool.returnToPool(con.sendFrame.bytes);
                    con.framePool.returnToPool(con.sendFrame);
                    con.sendFrame = null;
                    con.sendOffset = 0;
                } else {
                    //System.out.println("send frame not complete:" + con.sendFrame);
                    con.sendOffset += missingFromFrame;
                    break;
                }
            }
            con.sendBuf.flip();

            //System.out.println(IOUtil.toDebugString("sendBuf", con.sendBuf));

            long bytesWritten = channel.write(con.sendBuf);
            // System.out.println("bytes written:"+bytesWritten);
            con.bytesWritten += bytesWritten;

            if (con.sendBuf.remaining() == 0 && con.sendFrame == null) {
                //System.out.println("unregister");
                // unregister
                int interestOps = sk.interestOps();
                if ((interestOps & SelectionKey.OP_WRITE) != 0) {
                    sk.interestOps(interestOps & ~SelectionKey.OP_WRITE);
                }
            } else {
                // System.out.println("register");
                // register OP_WRITE
                sk.interestOps(sk.interestOps() | SelectionKey.OP_WRITE);
            }

            compactOrClear(con.sendBuf);
        }

        private void onRead(SelectionKey sk) throws IOException {
            SocketChannel channel = (SocketChannel) sk.channel();
            Connection con = (Connection) sk.attachment();
            con.onReadEvents++;

            int bytesRead = channel.read(con.receiveBuf);
            if (bytesRead == -1)
                throw new IOException("Channel " + channel.socket().getInetAddress() + " closed on the other side");
            con.bytesRead += bytesRead;

            con.receiveBuf.flip();
            boolean dirty = false;
            try {
                while (con.receiveBuf.remaining() > 0) {
                    if (con.receiveFrame == null) {
                        // not enough bytes available for the frame size; we are done.
                        if (con.receiveBuf.remaining() < INT_AS_BYTES) break;

                        con.receiveFrame = con.framePool.takeFromPool();
                        con.receiveFrame.length = con.receiveBuf.getInt();
                        if (con.receiveFrame.length < 0)
                            throw new IOException("Frame length can't be negative. Found:" + con.receiveFrame.length);

                        con.receiveFrame.bytes = con.byteArrayPool.takeFromPool(con.receiveFrame.length);
                    }

                    int missingFromFrame = con.receiveFrame.length - con.receiveOffset;
                    int bytesToRead = con.receiveBuf.remaining() < missingFromFrame ? con.receiveBuf.remaining() : missingFromFrame;

                    con.receiveBuf.get(con.receiveFrame.bytes, con.receiveOffset, bytesToRead);
                    con.receiveOffset += bytesToRead;
                    if (con.receiveOffset == con.receiveFrame.length) {
                        // we have fully loaded a frame.
                        dirty = true;
                        con.readFrames++;
                        con.pending.add(con.receiveFrame);
                        con.receiveFrame = null;
                        con.receiveOffset = 0;
                    }
                }
            } finally {
                compactOrClear(con.receiveBuf);
            }

            if (dirty) onWrite(sk);
        }

        private void shutdown() {
            acceptThread.interrupt();
            try {
                acceptThread.selector.close();
            } catch (IOException e) {
            }
        }
    }

    private static class Connection {
        SocketChannel channel;

        final ByteArrayPool byteArrayPool;
        final FramePool framePool;

        ByteBuffer receiveBuf;
        Frame receiveFrame;
        long onReadEvents;
        int receiveOffset;
        long readFrames;
        long bytesRead;

        long bytesWritten;
        final ArrayDeque<Frame> pending = new ArrayDeque<>();
        int onWriteEvents;
        int sendOffset;
        Frame sendFrame;
        ByteBuffer sendBuf;

        Connection(boolean objectPoolingEnabled) {
            byteArrayPool = new ByteArrayPool(objectPoolingEnabled);
            framePool = new FramePool(objectPoolingEnabled);
        }
    }

    private class AcceptThread extends Thread {
        private Selector selector;

        AcceptThread() {
            super("AcceptThread");
        }

        @Override
        public void run() {
            try {
                loop();
            } catch (Exception e) {
                if (!stopping) {
                    log(e);
                }
            }
        }

        private void shutdown() {
            acceptThread.interrupt();
            try {
                acceptThread.selector.close();
            } catch (IOException e) {
            }
        }

        private void loop() throws IOException {
            selector = Selector.open(); // selector is open here

            serverSocket.register(selector, SelectionKey.OP_ACCEPT, null);
            for (; ; ) {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectedKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey sk = iterator.next();
                    iterator.remove();

                    if (sk.isAcceptable()) onAccept();
                }
            }
        }

        private void onAccept() throws IOException {
            SocketChannel clientChannel = serverSocket.accept();
            log("Accepted: " + clientChannel.getLocalAddress());
            ServerThread ioThread = nextIOThread();
            ioThread.newChannels.add(clientChannel);
            ioThread.selector.wakeup();
        }
    }

    private static void log(String str) {
        System.out.println(str);
    }

    private static void log(Exception e) {
        e.printStackTrace();
    }

    public static class Context {
        private int serverThreadCount = max(4, Runtime.getRuntime().availableProcessors() / 2);
        private String bindAddress = "0.0.0.0";
        private int startPort = 1111;
        private int receiveBufferSize = 256 * 1024;
        private int sendBufferSize = 256 * 1024;
        private boolean tcpNoDelay = true;
        private boolean objectPoolingEnabled = true;
        private boolean optimizeSelector = true;
        private boolean directBuffers = true;
        private boolean selectorSpin = false;

        public Context selectorSpin(boolean selectorSpin) {
            this.selectorSpin = selectorSpin;
            return this;
        }

        public Context serverThreadCount(int serverThreadCount) {
            this.serverThreadCount = serverThreadCount;
            return this;
        }

        public Context bindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public Context startPort(int startPort) {
            this.startPort = startPort;
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

        public Context objectPoolingEnabled(boolean objectPoolingEnabled) {
            this.objectPoolingEnabled = objectPoolingEnabled;
            return this;
        }

        public Context optimizeSelector(boolean optimizeSelector) {
            this.optimizeSelector = optimizeSelector;
            return this;
        }

        public Context directBuffers(boolean directBuffers) {
            this.directBuffers = directBuffers;
            return this;
        }
    }
}
