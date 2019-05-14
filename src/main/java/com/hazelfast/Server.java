package com.hazelfast;

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

import static com.hazelfast.Util.compactOrClear;

public class Server {

    private final AtomicInteger ioThreadId = new AtomicInteger(0);
    private ServerSocketChannel serverSocket;
    private InetSocketAddress serverAddress;
    private IOThread[] ioThreads;
    private AcceptThread acceptThread;
    private final AtomicInteger nextIOThreadId = new AtomicInteger();
    private volatile boolean stopping = false;
    private final int ioThreadCount;
    private final String hostname;
    private final int port;
    private final int receiveBufferSize;
    private final int sendBufferSize;
    private final boolean tcpNoDelay;
    private final boolean objectPoolingEnabled;
    private final boolean optimizeSelector;
    private final boolean directBuffers;

    public Server(Context context) {
        this.ioThreadCount = context.ioThreadCount;
        this.hostname = context.hostname;
        this.port = context.port;
        this.receiveBufferSize = context.receiveBufferSize;
        this.sendBufferSize = context.sendBufferSize;
        this.tcpNoDelay = context.tcpNoDelay;
        this.objectPoolingEnabled = context.objectPoolingEnabled;
        this.optimizeSelector = context.optimizeSelector;
        this.directBuffers = context.directBuffers;
    }

    public int ioThreadCount() {
        return ioThreadCount;
    }

    public String hostname() {
        return hostname;
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
        for (IOThread ioThread : ioThreads) {
            ioThread.shutdown();
        }
    }

    public void start() throws IOException {
        serverSocket = ServerSocketChannel.open();
        serverAddress = new InetSocketAddress(hostname, port);
        serverSocket.bind(serverAddress);

        serverSocket.socket().setReceiveBufferSize(receiveBufferSize);
        if (serverSocket.socket().getReceiveBufferSize() != receiveBufferSize) {
            System.out.println("socket doesn't have expected receiveBufferSize, expected:"
                    + receiveBufferSize + " actual:" + serverSocket.socket().getReceiveBufferSize());
        }

        serverSocket.configureBlocking(false);

        this.ioThreads = new IOThread[ioThreadCount];
        for (int k = 0; k < ioThreadCount; k++) {
            ioThreads[k] = new IOThread();
            ioThreads[k].start();
        }
        this.acceptThread = new AcceptThread();
        acceptThread.start();
    }

    private IOThread nextIOThread() {
        int next = nextIOThreadId.getAndIncrement() % ioThreadCount;
        return ioThreads[next];
    }

    private class IOThread extends Thread {
        private final Selector selector;
        private final ConcurrentLinkedQueue<SocketChannel> newChannels = new ConcurrentLinkedQueue<>();

        private IOThread() throws IOException {
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
                selector.select();
                registerNewChannels();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey sk = iterator.next();
                    iterator.remove();

                    if (!sk.isValid()) {
                        sk.channel().close();
                        continue;
                    }

                    if (sk.isReadable()) {
                        onRead(sk);
                    }

                    if (sk.isWritable()) {
                        onWrite(sk);
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

                Session session = new Session();
                session.receiveBuffer = directBuffers
                        ? ByteBuffer.allocateDirect(receiveBufferSize)
                        : ByteBuffer.allocate(receiveBufferSize);
                session.sendBuffer = directBuffers
                        ? ByteBuffer.allocateDirect(sendBufferSize)
                        : ByteBuffer.allocate(sendBufferSize);

                // System.out.println(toDebugString("create sendBuffer", session.sendBuffer));

                session.channel = channel;

                channel.register(selector, SelectionKey.OP_READ, session);

                // log(getName() + " waiting for data on " + session.channel.socket().getInetAddress());
            }
        }

        private void onWrite(SelectionKey sk) throws IOException {
            SocketChannel clientChannel = (SocketChannel) sk.channel();
            Session session = (Session) sk.attachment();
            session.onWriteEvents++;

            //System.out.println(toDebugString("sendBuffer", session.sendBuffer));

            for (; ; ) {
                Request request = session.processingRequest;
                if (request == null) {
                    request = session.pendingRequests.poll();
                    if (request == null) {
                        break;
                    }
                }

                session.processingRequest = request;
                if (session.sendBuffer.remaining() < 4) {
                    break;
                }

                session.sendBuffer.putInt(request.length);

                int sendBufferRemaining = session.sendBuffer.remaining();
                int remaining = request.length - session.sendOffset;
                boolean complete = false;
                if (sendBufferRemaining <= remaining) {
                    remaining = sendBufferRemaining;
                } else {
                    complete = true;
                }

                session.sendBuffer.put(request.bytes, session.sendOffset, remaining);

                if (complete) {
                    if (objectPoolingEnabled && (session.pooledBytes == null || session.pooledBytes.length < request.bytes.length)) {
                        session.pooledBytes = request.bytes;
                    }
                    request.bytes = null;
                    request.length = 0;
                    if (objectPoolingEnabled) {
                        session.pooledRequests.add(request);
                    }
                    session.processingRequest = null;
                    session.sendOffset = 0;
                } else {
                    session.sendOffset += remaining;
                    break;
                }
            }

            session.sendBuffer.flip();

            int written = clientChannel.write(session.sendBuffer);
            //System.out.println("Written:" + written + " bytes to socket");

            if (session.sendBuffer.remaining() == 0 || session.processingRequest != null) {
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

            compactOrClear(session.sendBuffer);
        }

        private void onRead(SelectionKey sk) throws IOException {
            SocketChannel clientChannel = (SocketChannel) sk.channel();
            Session session = (Session) sk.attachment();
            session.onReadEvents++;

            int read = clientChannel.read(session.receiveBuffer);
            if (read == -1) {
                log("Closing: " + clientChannel.socket().getInetAddress());
                log(clientChannel.socket().getInetAddress() + " bytes read:" + session.bytesRead);
                log(clientChannel.socket().getInetAddress() + " onReadEvents:" + session.onReadEvents);
                clientChannel.close();
                return;
            }

            // log("read:" + read + " bytes");

            session.receiveBuffer.flip();
            boolean added = false;
            try {
                while (session.receiveBuffer.remaining() > 0) {
                    if (session.receivingRequest == null) {
                        if (session.receiveBuffer.remaining() < 4) {
                            break;
                        }
                        session.receivingRequest = session.pooledRequests.poll();
                        if (session.receivingRequest == null) {
                            session.receivingRequest = new Request();
                        }
                        session.receivingRequest.length = session.receiveBuffer.getInt();
                        if (objectPoolingEnabled && session.pooledBytes != null && session.pooledBytes.length >= session.receivingRequest.length) {
                            session.receivingRequest.bytes = session.pooledBytes;
                            session.pooledBytes = null;
                        } else {
                            session.receivingRequest.bytes = new byte[session.receivingRequest.length];
                        }
                    }

                    int missing = session.receivingRequest.length - session.receiveOffset;
                    session.receiveBuffer.get(session.receivingRequest.bytes, session.receiveOffset, missing);
                    session.receiveOffset += missing;
                    if (session.receiveOffset == session.receivingRequest.length) {
                        session.receiveOffset = 0;
                        session.resLen = 0;
                        added = true;

                        //      System.out.println("Received message");
                        session.readMessages++;
                        session.pendingRequests.add(session.receivingRequest);
                        session.receivingRequest = null;
                    }
                }
            } finally {
                compactOrClear(session.receiveBuffer);
            }

            if (added) {
                onWrite(sk);
            }
        }

        public void shutdown() {
            acceptThread.interrupt();
            try {
                acceptThread.selector.close();
            } catch (IOException e) {
            }
        }
    }

    private static class Session {
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

        public void shutdown() {
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
            IOThread ioThread = nextIOThread();
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
        private int ioThreadCount = 10;
        private String hostname = "localhost";
        private int port = 1111;
        private int receiveBufferSize = 512 * 1024;
        private int sendBufferSize = 512 * 1024;
        private boolean tcpNoDelay = true;
        private boolean objectPoolingEnabled = true;
        private boolean optimizeSelector = true;
        private boolean directBuffers = true;

        public Context setIoThreadCount(int ioThreadCount) {
            this.ioThreadCount = ioThreadCount;
            return this;
        }

        public Context setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Context setPort(int port) {
            this.port = port;
            return this;
        }

        public Context setReceiveBufferSize(int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        public Context setSendBufferSize(int sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
            return this;
        }

        public Context setTcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        public Context setObjectPoolingEnabled(boolean objectPoolingEnabled) {
            this.objectPoolingEnabled = objectPoolingEnabled;
            return this;
        }

        public Context setOptimizeSelector(boolean optimizeSelector) {
            this.optimizeSelector = optimizeSelector;
            return this;
        }

        public Context setDirectBuffers(boolean directBuffers) {
            this.directBuffers = directBuffers;
            return this;
        }
    }

}
