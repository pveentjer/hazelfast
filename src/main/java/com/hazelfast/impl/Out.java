package com.hazelfast.impl;

import java.nio.ByteBuffer;

public class Out {

    private ByteBuffer bb;

    public void putString(String s){
        bb.putInt(s.length());
    }

    public void putLong(long result) {
        bb.putLong(result);
    }

    public void putByte(byte b) {
        bb.put(b);
    }
}
