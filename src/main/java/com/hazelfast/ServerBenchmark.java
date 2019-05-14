package com.hazelfast;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = {"-Xms2G", "-Xmx2G"})
//@Warmup(iterations = 3)
//@Measurement(iterations = 8)
public class ServerBenchmark {

    private Client client;
    private Server server;
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

    @Benchmark
    public void benchmark(Blackhole bh) throws IOException {
        client.writeRequest("f");
        client.readResponse();
    }
}
