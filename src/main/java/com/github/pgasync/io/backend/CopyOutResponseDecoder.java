package com.github.pgasync.io.backend;

import com.github.pgasync.io.Decoder;
import com.github.pgasync.message.backend.CopyOutResponse;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Decodes backend 'H' (CopyOutResponse) message.
 *
 * <pre>
 * CopyOutResponse (B)
 *   Byte1('H')   — message type
 *   Int32        — length (including self)
 *   Int8         — overall format (0 = text, 1 = binary)
 *   Int16        — number of columns
 *   Int16[n]     — per-column format codes
 * </pre>
 */
public class CopyOutResponseDecoder implements Decoder<CopyOutResponse> {

    @Override
    public byte getMessageId() {
        return 'H';
    }

    @Override
    public CopyOutResponse read(ByteBuffer buffer, int contentLength, Charset encoding) {
        int overallFormat = buffer.get() & 0xFF;
        int numColumns = buffer.getShort() & 0xFFFF;
        int[] columnFormats = new int[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columnFormats[i] = buffer.getShort() & 0xFFFF;
        }
        return new CopyOutResponse(overallFormat, columnFormats);
    }
}

