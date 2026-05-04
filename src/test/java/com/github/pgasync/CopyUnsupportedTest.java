package com.github.pgasync;

import com.pgasync.Connectible;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CopyUnsupportedTest {

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule();

    @Test
    public void shouldFailWithClearMessageOnCopyIn() throws Exception {
        assertCopyUnsupported("CREATE TEMP TABLE COPY_TEST(ID INT8); COPY COPY_TEST(ID) FROM STDIN", true,
                "COPY IN is not supported yet");
    }

    @Test
    public void shouldFailWithClearMessageOnCopyOut() throws Exception {
        assertCopyUnsupported("COPY (SELECT 1) TO STDOUT", false, "COPY OUT is not supported yet");
    }

    private static void assertCopyUnsupported(String sql, boolean script, String expectedMessagePart) throws Exception {
        Connectible pool = dbr.builder.pool();
        try {
            if (script) {
                pool.completeScript(sql).get();
            } else {
                pool.completeQuery(sql).get();
            }
            fail("Expected COPY command to fail with UnsupportedOperationException");
        } catch (Exception ex) {
            Throwable rootCause = rootCause(ex);
            assertTrue("Expected UnsupportedOperationException but got: " + rootCause,
                    rootCause instanceof UnsupportedOperationException);
            assertTrue("Expected message to contain: " + expectedMessagePart + " but was: " + rootCause.getMessage(),
                    rootCause.getMessage() != null && rootCause.getMessage().contains(expectedMessagePart));
            assertEquals(1, pool.completeQuery("SELECT 1").get().at(0).getInt(0).intValue());
        } finally {
            pool.close().get();
        }
    }

    private static Throwable rootCause(Throwable th) {
        Throwable current = th;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
