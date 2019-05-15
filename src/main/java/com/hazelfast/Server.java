package com.hazelfast;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelfast.Util.compactOrClear;

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
        this.port = context.port;
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
        Server server = new Server(new Context());
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

        private ServerThread() throws IOException {
            super("IOThread#" + ioThreadId.getAndIncrement());
            setDaemon(true);
            selector = optimizeSelector ? Util.newSelector() : Selector.open();
        }

        @Override
        public void run() {
            log(getName() + " running");
            try {
                loop();
            } catch (Exception e) {
                if (!stopping) {
                    log(e);
                }
            }
        }

        private void loop() throws IOException {
            for (; ; ) {
                int selectedKeys = selectorSpin ? selector.selectNow() : selector.select();
                registerNewChannels();
                if (selectedKeys == 0) continue;

                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey sk = iterator.next();
                    iterator.remove();

                    try {
                        if (sk.isReadable()) onRead(sk);
                        if (sk.isWritable()) onWrite(sk);
                    } catch (CancelledKeyException e) {
                        sk.channel().close();
                    }
                }
            }
        }

        private void registerNewChannels() throws IOException {
            for (; ; ) {
                SocketChannel channel = newChannels.poll();
                if (channel == null) {
                    break;
                }
                channel.configureBlocking(false);

                channel.socket().setReceiveBufferSize(receiveBufferSize);
                if (channel.socket().getReceiveBufferSize() != receiveBufferSize) {
                    System.out.println("socket doesn't have expected receiveBufferSize, expected:"
                            + receiveBufferSize + " actual:" + channel.socket().getReceiveBufferSize());
                }

                channel.socket().setSendBufferSize(sendBufferSize);
                if (channel.socket().getSendBufferSize() != sendBufferSize) {
                    System.out.println("socket doesn't have expected sendBufferSize, expected:"
                            + sendBufferSize + " actual:" + channel.socket().getSendBufferSize());
                }

                channel.socket().setTcpNoDelay(tcpNoDelay);

                Connection con = new Connection();
                con.receiveBuffer = directBuffers
                        ? ByteBuffer.allocateDirect(receiveBufferSize)
                        : ByteBuffer.allocate(receiveBufferSize);
                con.sendBuffer = directBuffers
                        ? ByteBuffer.allocateDirect(sendBufferSize)
                        : ByteBuffer.allocate(sendBufferSize);

                // System.out.println(toDebugString("create sendBuffer", connection.sendBuffer));

                con.channel = channel;

                channel.register(selector, SelectionKey.OP_READ, con);

                // log(getName() + " waiting for data on " + connection.channel.socket().getInetAddress());
            }
        }

        private void onWrite(SelectionKey sk) throws IOException {
            SocketChannel clientChannel = (SocketChannel) sk.channel();
            Connection con = (Connection) sk.attachment();
            con.onWriteEvents++;

            //System.out.println(toDebugString("sendBuffer", con.sendBuffer));

            for (; ; ) {
                Request request = con.processingRequest;
                if (request == null) {
                    request = con.pendingRequests.poll();
                    if (request == null) {
                        break;
                    }
                }

                con.processingRequest = request;
                if (con.sendBuffer.remaining() < 4) {
                    break;
                }

                con.sendBuffer.putInt(request.length);

                int sendBufferRemaining = con.sendBuffer.remaining();
                int remaining = request.length - con.sendOffset;
                boolean complete = false;
                if (sendBufferRemaining <= remaining) {
                    remaining = sendBufferRemaining;
                } else {
                    complete = true;
                }

                con.sendBuffer.put(request.bytes, con.sendOffset, remaining);

                if (complete) {
                    if (objectPoolingEnabled && (con.pooledBytes == null || con.pooledBytes.length < request.bytes.length)) {
                        con.pooledBytes = request.bytes;
                    }
                    request.bytes = null;
                    request.length = 0;
                    if (objectPoolingEnabled) {
                        con.pooledRequests.add(request);
                    }
                    con.processingRequest = null;
                    con.sendOffset = 0;
                } else {
                    con.sendOffset += remaining;
                    break;
                }
            }

            con.sendBuffer.flip();

            int written = clientChannel.write(con.sendBuffer);
            //System.out.println("Written:" + written + " bytes to socket");

            if (con.sendBuffer.remaining() == 0 || con.processingRequest != null) {
                // System.out.println("unregister");
                // unregister
                int interestOps = sk.interestOps();
                if ((interestOps & SelectionKey.OP_WRITE) != 0) {
                    sk.interestOps(interestOps & ~SelectionKey.OP_WRITE);
                }
            } else {
                //  System.out.println("register");
                // register OP_WRITE
                sk.interestOps(sk.interestOps() | SelectionKey.OP_WRITE);
            }

            compactOrClear(con.sendBuffer);
        }

        private void onRead(SelectionKey sk) throws IOException {
            SocketChannel clientChannel = (SocketChannel) sk.channel();
            Connection con = (Connection) sk.attachment();
            con.onReadEvents++;

            int read = clientChannel.read(con.receiveBuffer);
            if (read == -1) {
                log("Closing: " + clientChannel.socket().getInetAddress());
                log(clientChannel.socket().getInetAddress() + " bytes read:" + con.bytesRead);
                log(clientChannel.socket().getInetAddress() + " onReadEvents:" + con.onReadEvents);
                clientChannel.close();
                return;
            }

            // log("read:" + read + " bytes");

            con.receiveBuffer.flip();
            boolean added = false;
            try {
                while (con.receiveBuffer.remaining() > 0) {
                    if (con.receivingRequest == null) {
                        if (con.receiveBuffer.remaining() < 4) {
                            break;
                        }
                        con.receivingRequest = con.pooledRequests.poll();
                        if (con.receivingRequest == null) {
                            con.receivingRequest = new Request();
                        }
                        con.receivingRequest.length = con.receiveBuffer.getInt();
                        if (objectPoolingEnabled && con.pooledBytes != null && con.pooledBytes.length >= con.receivingRequest.length) {
                            con.receivingRequest.bytes = con.pooledBytes;
                            con.pooledBytes = null;
                        } else {
                            con.receivingRequest.bytes = new byte[con.receivingRequest.length];
                        }
                    }

                    int missing = con.receivingRequest.length - con.receiveOffset;
                    con.receiveBuffer.get(con.receivingRequest.bytes, con.receiveOffset, missing);
                    con.receiveOffset += missing;
                    if (con.receiveOffset == con.receivingRequest.length) {
                        con.receiveOffset = 0;
                        con.resLen = 0;
                        added = true;

                        //      System.out.println("Received message");
                        con.readMessages++;
                        con.pendingRequests.add(con.receivingRequest);
                        con.receivingRequest = null;
                    }
                }
            } finally {
                compactOrClear(con.receiveBuffer);
            }

            if (added) onWrite(sk);

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
        final ArrayDeque<Request> pendingRequests = new ArrayDeque<>();

        byte[] pooledBytes;
        final ArrayDeque<Request> pooledRequests = new ArrayDeque<>();

        int onWriteEvents;
        long resLen = -1;
        ByteBuffer sendBuffer;

        int receiveOffset;
        long readMessages;
        long bytesRead;
        long onReadEvents;
        SocketChannel channel;

        int sendOffset;
        ByteBuffer receiveBuffer;
        Request receivingRequest;
        Request processingRequest;
    }

    private static class Request {
        int length;
        byte[] bytes;
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

                    if (sk.isAcceptable()) {
                        onAccept();
                    }
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
        private int serverThreadCount = 10;
        private String bindAddress = "0.0.0.0";
        private int port = 1111;
        private int receiveBufferSize = 512 * 1024;
        private int sendBufferSize = 512 * 1024;
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

        public Context port(int port) {
            this.port = port;
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
