package com.hazelfast;

import java.nio.ByteBuffer;

import static com.hazelfast.impl.CountersData.FUNCTION_CAS;
import static com.hazelfast.impl.CountersData.FUNCTION_GET;
import static com.hazelfast.impl.CountersData.FUNCTION_INC;
import static com.hazelfast.impl.CountersData.FUNCTION_SET;
import static com.hazelfast.impl.IOUtil.LONG_AS_BYTES;
import static com.hazelfast.impl.DataStructures.COUNTER;

public final class Counters {

     private final Client client;

    public Counters(Client client) {
        this.client = client;
    }

    public long get(long id) {
        ByteBuffer b = client.sendBuf;
        b.putInt(1 + 1 + LONG_AS_BYTES);
        b.put(COUNTER);
        b.put(FUNCTION_GET);
        b.putLong(id);
        client.writeAndFlush();
        return 0;
    }

    public long set(long id, long value) {
        ByteBuffer b = client.sendBuf;
        b.putInt(1 + 1 + LONG_AS_BYTES + LONG_AS_BYTES);
        b.put(COUNTER);
        b.put(FUNCTION_SET);
        b.putLong(id);
        b.putLong(value);
        client.writeAndFlush();
        return 0;
    }

    public long inc(long id) {
        return inc(id, 1);
    }

    public long inc(long id, int amount) {
        ByteBuffer b = client.sendBuf;
        b.putInt(1 + 1 + LONG_AS_BYTES + LONG_AS_BYTES);
        b.put(COUNTER);
        b.put(FUNCTION_INC);
        b.putLong(id);
        b.putLong(amount);
        client.writeAndFlush();
        return 0;
    }

    public boolean cas(long id, long oldValue, long newValue) {
        ByteBuffer b = client.sendBuf;
        b.putInt(1 + 1 + LONG_AS_BYTES + LONG_AS_BYTES + LONG_AS_BYTES);
        b.put(COUNTER);
        b.put(FUNCTION_CAS);
        b.putLong(id);
        b.putLong(oldValue);
        b.putLong(newValue);
        client.writeAndFlush();
        return true;
    }

}
