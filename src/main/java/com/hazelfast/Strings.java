package com.hazelfast;

import java.nio.ByteBuffer;

public class Strings {
    protected final byte ID = DataTypes.STRINGS;
    protected final byte OP_GET = 1;
    protected final byte OP_SET = 2;
    private final Client client;

    public Strings(Client client) {
        this.client = client;
    }

    public void set(String key, String value) {
        ByteBuffer b = client.sendBuffer;
        b.put(ID);
        b.put(OP_SET);

        client.write();

        //return 0;
    }

    public String get(String key) {
        ByteBuffer b = client.sendBuffer;
        b.put(ID);
        b.put(OP_GET);

        client.write();
        return null;
    }


}
