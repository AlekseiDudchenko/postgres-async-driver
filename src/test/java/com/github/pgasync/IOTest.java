package com.github.pgasync;

import com.github.pgasync.io.IO;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class IOTest {

    @Test
    public void nullCharacters() {
        var stringWithNulls = new String(new char[]{'t', 'h', (char) 0x00, 'i', 's', ' ', (char) 0x00, 'n'});
        var utf8Bytes = stringWithNulls.getBytes(StandardCharsets.UTF_8);
        assertEquals(utf8Bytes.length, stringWithNulls.length());
        var bb = ByteBuffer.allocate(16);
        IO.putCString(bb, stringWithNulls, StandardCharsets.UTF_8);
        assertEquals(stringWithNulls.length() - 2/* Null characters*/ + 1/*trailing 0x00*/, bb.position());
    }
}
