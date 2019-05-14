package com.hazelfast;

import java.nio.ByteBuffer;

public final class Counters {

    protected final byte ID = DataTypes.COUNTER;
    protected final byte OP_GET = 1;
    protected final byte OP_SET = 2;
    protected final byte OP_INC = 3;
    protected final byte OP_CAS = 4;

    private final Client client;

    public Counters(Client client) {
        this.client = client;
    }

    public long get(long id) {
        ByteBuffer b = client.sendBuffer;
        b.put(ID);
        b.put(OP_GET);
        b.putLong(id);
        client.write();

        return 0;
    }

    public long set(long id) {
        ByteBuffer b = client.sendBuffer;
        b.put(ID);
        b.put(OP_SET);
        b.putLong(id);
        client.write();
        return 0;
    }

    public long inc(long id) {
        return inc(id, 1);
    }

    public long inc(long id, int amount) {
        ByteBuffer b = client.sendBuffer;
        b.put(ID);
        b.put(OP_INC);
        b.putLong(id);
        b.putLong(amount);
        client.write();
        return 0;
    }

    public boolean cas(long id, long oldValue, long newValue) {
        ByteBuffer b = client.sendBuffer;
        b.put(ID);
        b.put(OP_CAS);
        b.putLong(id);
        b.putLong(oldValue);
        b.putLong(newValue);
        client.write();
        return true;
    }
}
