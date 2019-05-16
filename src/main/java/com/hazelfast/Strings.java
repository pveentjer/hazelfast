package com.hazelfast;

import com.hazelfast.impl.DataStructures;
import com.hazelfast.impl.StringsData;

import java.nio.ByteBuffer;

import static com.hazelfast.impl.DataStructures.STRINGS;
import static com.hazelfast.impl.StringsData.FUNCTION_GET;
import static com.hazelfast.impl.StringsData.FUNCTION_SET;

public class Strings {
    private final Client client;

    public Strings(Client client) {
        this.client = client;
    }

    public void set(String key, String value) {
        ByteBuffer b = client.sendBuffer;
        b.put(STRINGS);
        b.put(FUNCTION_SET);

        client.write();

        //return 0;
    }

    public String get(String key) {
        ByteBuffer b = client.sendBuffer;
        b.put(STRINGS);
        b.put(FUNCTION_GET);

        client.write();
        return null;
    }


}
