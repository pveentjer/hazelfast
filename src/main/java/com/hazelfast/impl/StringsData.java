package com.hazelfast.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class StringsData {
    public static final byte FUNCTION_GET = 1;
    public static final byte FUNCTION_SET = 2;

    // todo: should be replaced by offheap data-structure
    // currently the key generates litter.
    private final Map<String, String> counters = new HashMap<>();
    private final StringBuilder s1 = new StringBuilder();
    private final StringBuilder s2 = new StringBuilder();

    public void process(In in, Out out) {
        byte function = in.getByte();
        switch (function) {
            case FUNCTION_GET:
                get(in, out);
                break;
            case FUNCTION_SET:
                set(in, out);
                break;
            default:
                throw new IllegalStateException("Unrecognized function:" + function);
        }
    }

    private void get(In in, Out out) {

    }

    private void set(In in, Out out) {

    }
}

