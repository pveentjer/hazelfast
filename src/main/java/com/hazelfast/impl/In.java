package com.hazelfast.impl;

import java.nio.ByteBuffer;

public class In {
    private ByteBuffer bb;


    public byte getByte(){
        return bb.get();
    }

    public int getInt(){
        return bb.getInt();
    }

    public long getLong(){
        return bb.getLong();
    }
}
