package com.hazelfast.impl;

public class DataStructures {
    public static final byte PING = 0;
    public static final byte COUNTER = 1;
    public static final byte STRINGS = 2;

    public CountersData counters = new CountersData();
    public StringsData strings = new StringsData();

    public void dispatch(In in, Out out) {
        byte type = in.getByte();
        switch (type) {
            case COUNTER:
                counters.process(in, out);
                break;
            case STRINGS:
                strings.process(in, out);
            default:
                throw new IllegalStateException("Unrecognized datastructure:" + type);
        }
    }
}
