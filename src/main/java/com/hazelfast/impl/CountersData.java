package com.hazelfast.impl;

import java.util.HashMap;
import java.util.Map;


public class CountersData {
    public static final byte FUNCTION_GET = 1;
    public static final byte FUNCTION_SET = 2;
    public static final byte FUNCTION_INC = 3;
    public static final byte FUNCTION_CAS = 4;


    // todo: should be replaced by offheap data-structure
    // currently the key generates litter.
    private final Map<Long, LongValue> counters = new HashMap<>();

    public void process(In in, Out out) {
        byte function = in.getByte();
        switch (function) {
            case FUNCTION_GET:
                get(in, out);
                break;
            case FUNCTION_SET:
                set(in, out);
                break;
            case FUNCTION_INC:
                inc(in, out);
                break;
            case FUNCTION_CAS:
                cas(in, out);
                break;
            default:
                throw new IllegalStateException("Unrecognized function:" + function);
        }
    }

    private void get(In in, Out out) {
        long id = in.getLong();
        LongValue v = counters.get(id);
        long result = v == null ? 0 : v.v;
        out.putLong(result);
    }

    private void set(In in, Out out) {
        long id = in.getLong();
        long newValue = in.getLong();
        LongValue v = counters.get(id);
        if (v == null) {
            v = new LongValue();
            counters.put(id, v);
        }
        v.v = newValue;
        out.putByte((byte)1);
    }

    private void inc(In in, Out out) {
        long id = in.getLong();
        long amount = in.getLong();
        LongValue v = counters.get(id);
        if (v == null) {
            v = new LongValue();
            counters.put(id, v);
        }
        v.v += amount;
        out.putLong(v.v);
    }

    private void cas(In in, Out out) {
        long id = in.getLong();
        long oldValue = in.getLong();
        long newValue = in.getLong();
        LongValue v = counters.get(id);
        if (v == null) {
            v = new LongValue();
            counters.put(id, v);
        }
        if (v.v == oldValue) {
            v.v = newValue;
            out.putByte((byte) 1);
        } else {
            out.putByte((byte) 0);
        }
    }

    private static class LongValue {
        private long v;
    }
}
