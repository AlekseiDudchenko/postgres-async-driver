package com.github.pgasync.message.backend;

import com.github.pgasync.message.Message;

/**
 * Backend message 'H' — server is about to send COPY OUT data to the client.
 * Sent in response to a COPY ... TO STDOUT command.
 *
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">PostgreSQL protocol message formats</a>
 */
public class CopyOutResponse implements Message {

    private final int overallFormat;
    private final int[] columnFormats;

    public CopyOutResponse(int overallFormat, int[] columnFormats) {
        this.overallFormat = overallFormat;
        this.columnFormats = columnFormats;
    }

    public int getOverallFormat() {
        return overallFormat;
    }

    public int[] getColumnFormats() {
        return columnFormats;
    }

    @Override
    public String toString() {
        return "CopyOutResponse(overallFormat=" + overallFormat + ", columns=" + columnFormats.length + ")";
    }
}
