package com.github.pgasync.message.backend;

import com.github.pgasync.message.Message;

/**
 * Backend message 'G' — server is ready to receive COPY IN data from the client.
 * Sent in response to a COPY ... FROM STDIN command.
 *
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">PostgreSQL protocol message formats</a>
 */
public class CopyInResponse implements Message {

    /** Overall data transfer format: 0 = text, 1 = binary. */
    private final int overallFormat;
    /** Per-column format codes (0 = text, 1 = binary). */
    private final int[] columnFormats;

    public CopyInResponse(int overallFormat, int[] columnFormats) {
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
        return "CopyInResponse(overallFormat=" + overallFormat + ", columns=" + columnFormats.length + ")";
    }
}

