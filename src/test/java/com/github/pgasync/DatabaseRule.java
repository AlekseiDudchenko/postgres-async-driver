package com.github.pgasync;

import static java.lang.System.getenv;

import com.github.pgasync.netty.NettyConnectibleBuilder;
import com.pgasync.Connectible;
import com.pgasync.ConnectibleBuilder;
import com.pgasync.ResultSet;
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Antti Laisi
 */
class DatabaseRule extends ExternalResource {

    private static PostgreSQLContainer<?> postgres;

    final ConnectibleBuilder builder;
    Connectible pool;

    DatabaseRule() {
        this(createPoolBuilder(1));
    }

    DatabaseRule(ConnectibleBuilder builder) {
        this.builder = builder;
        if (builder instanceof EmbeddedConnectionPoolBuilder) {
            String port = System.getProperty("asyncpg.test.postgres.port");
            if (port != null && !port.isBlank()) {
                builder.hostname("localhost");
                builder.port(Integer.parseInt(port));
            } else {
                if (postgres == null) {
                    postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:11.1"))
                            .withDatabaseName("async-pg")
                            .withUsername("async-pg")
                            .withPassword("async-pg");
                    postgres.start();

                    System.out.printf("Started postgres to %s:%d%n", postgres.getHost(), postgres.getFirstMappedPort());
                }
                builder.hostname(postgres.getHost());
                builder.port(postgres.getFirstMappedPort());
            }
        }
    }

    @Override
    protected void before() {
        if (pool == null) {
            pool = builder.pool();
        }
    }

    @Override
    protected void after() {
        if (pool != null) {
            try {
                pool.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    ResultSet query(String sql) {
        return block(pool().completeQuery(sql));
    }

    ResultSet query(String sql, List<?> params) {
        return block(pool().completeQuery(sql, params.toArray()));
    }

    Collection<ResultSet> script(String sql) {
        return block(pool().completeScript(sql));
    }

    private <T> T block(CompletableFuture<T> future) {
        try {
            return future.get(5_0000000, TimeUnit.SECONDS);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    Connectible pool() {
        before();
        return pool;
    }

    Connectible plain() {
        before();
        return pool;
    }

    static class EmbeddedConnectionPoolBuilder extends NettyConnectibleBuilder {
        EmbeddedConnectionPoolBuilder() {
            database("async-pg");
            username("async-pg");
            password("async-pg");
            encoding(System.getProperty("file.encoding", "utf-8"));
        }
    }

    static Connectible createPool(int size) {
        return createPoolBuilder(size).pool();
    }

    static ConnectibleBuilder createPoolBuilder(int size) {
        String db = getenv("PG_DATABASE");
        String user = getenv("PG_USERNAME");
        String pass = getenv("PG_PASSWORD");

        if (db == null && user == null && pass == null) {
            return new EmbeddedConnectionPoolBuilder().maxConnections(size);
        } else {
            return new EmbeddedConnectionPoolBuilder()
                    .database(envOrDefault("PG_DATABASE", "postgres"))
                    .username(envOrDefault("PG_USERNAME", "postgres"))
                    .password(envOrDefault("PG_PASSWORD", "postgres"))
                    .ssl(true)
                    .maxConnections(size);
        }
    }


    static String envOrDefault(String var, String def) {
        String value = getenv(var);
        return value != null ? value : def;
    }
}
