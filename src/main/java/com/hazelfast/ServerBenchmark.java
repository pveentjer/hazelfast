package com.hazelfast;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = {"-Xms2G", "-Xmx2G"})
//@Warmup(iterations = 3)
//@Measurement(iterations = 8)
public class ServerBenchmark {

    private Client client;
    private Server server;
    private byte[] message = "foo".getBytes();
//    public static void main(String[] args) throws RunnerException {
//        Options opt = new OptionsBuilder()
//                .include(ServerBenchmark.class.getSimpleName())
//                .addProfiler(GCProfiler.class)
//                .detectJvmArgs()
//                .build();
//
//        new Runner(opt).run();
//    }
    @Setup
    public void setup() throws Exception {
        server = new Server(new Server.Context());
        server.start();
        client = new Client(new Client.Context());
        client.start();
    }

    @TearDown
    public void teardown() throws IOException {
        server.stop();
        client.stop();
    }

//    @Benchmark
//    public void benchmark() throws IOException {
//        client.writeAndFlush(message);
//        client.readResponse();
//    }

    @Benchmark
    public void pipelinedBenchmark() throws IOException {
        client.writeAndFlush(message);
        client.writeAndFlush(message);
        client.readResponse();
        client.readResponse();
    }
}
