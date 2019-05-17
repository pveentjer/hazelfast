package com.hazelfast.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class FramePoolTest {

    @Test
    public void take_whenNotPooled() {
        FramePool pool = new FramePool(true);
        assertNotNull(pool.takeFromPool());
    }

    @Test
    public void take_whenPooled() {
        FramePool pool = new FramePool(true);

        Frame frame1 = new Frame();
        Frame frame2 = new Frame();

        pool.returnToPool(frame1);
        pool.returnToPool(frame2);

        assertEquals(frame1, pool.takeFromPool());
        assertEquals(frame2, pool.takeFromPool());
        Frame found = pool.takeFromPool();
        assertNotSame(frame1, found);
        assertNotSame(frame2, found);
    }
}
