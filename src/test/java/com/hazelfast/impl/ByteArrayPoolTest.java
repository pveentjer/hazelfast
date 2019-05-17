package com.hazelfast.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ByteArrayPoolTest {

    private ByteArrayPool pool;

    @BeforeEach
    public void beforeEach() {
        pool = new ByteArrayPool(true);
    }

    @Test
    public void take_whenPooled_smallerSize() {
        byte[] b = new byte[128];
        pool.returnToPool(b);

        assertSame(b, pool.takeFromPool(127));
    }

    @Test
    public void test_whenPooled_sameSize() {
        byte[] b = new byte[128];
        pool.returnToPool(b);

        assertSame(b, pool.takeFromPool(128));
    }

    @Test
    public void test_whenNotPooled_thenReturnPowerOfTwo() {
        byte[] bytes = pool.takeFromPool(129);
        assertEquals(256, bytes.length);
    }

    @Test
    public void test_whenNotPooled_sameSize() {
        byte[] b = new byte[128];
        pool.returnToPool(b);

        assertSame(b, pool.takeFromPool(128));
    }

    @Test
    public void test_range() {
        for (int k = 0; k < 10000; k++) {
            byte[] b1 = pool.takeFromPool(k);
            pool.returnToPool(b1);
            byte[] b2 = pool.takeFromPool(k);
            assertEquals(b1, b2);
            assertTrue(b1.length >= k);
        }
    }

    @Test()
    public void test_add_whenNotPowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> {
            pool.returnToPool(new byte[127]);
        });
    }

    @Test()
    public void test_add_whenNull() {
        assertThrows(NullPointerException.class, () -> {
            pool.returnToPool(null);
        });
    }
}
