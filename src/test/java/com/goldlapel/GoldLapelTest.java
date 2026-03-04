package com.goldlapel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;


class FindBinaryTest {

    @Test
    void envVarOverride(@TempDir Path tmp) throws IOException {
        Path binary = tmp.resolve("goldlapel");
        Files.createFile(binary);

        String orig = System.getenv("GOLDLAPEL_BINARY");
        try {
            setEnv("GOLDLAPEL_BINARY", binary.toString());
            assertEquals(binary.toString(), GoldLapel.findBinary());
        } finally {
            setEnv("GOLDLAPEL_BINARY", orig);
        }
    }

    @Test
    void envVarMissingFileThrows() {
        String orig = System.getenv("GOLDLAPEL_BINARY");
        try {
            setEnv("GOLDLAPEL_BINARY", "/nonexistent/goldlapel");
            RuntimeException ex = assertThrows(RuntimeException.class, GoldLapel::findBinary);
            assertTrue(ex.getMessage().contains("GOLDLAPEL_BINARY"));
        } finally {
            setEnv("GOLDLAPEL_BINARY", orig);
        }
    }

    @Test
    void notFoundThrows() {
        String orig = System.getenv("GOLDLAPEL_BINARY");
        String origPath = System.getenv("PATH");
        try {
            removeEnv("GOLDLAPEL_BINARY");
            setEnv("PATH", "/nonexistent-dir-for-test");
            RuntimeException ex = assertThrows(RuntimeException.class, GoldLapel::findBinary);
            assertTrue(ex.getMessage().contains("Gold Lapel binary not found"));
        } finally {
            setEnv("GOLDLAPEL_BINARY", orig);
            setEnv("PATH", origPath);
        }
    }

    // Reflective env var manipulation for testing (same approach as other wrappers)
    @SuppressWarnings("unchecked")
    private static void setEnv(String key, String value) {
        try {
            java.util.Map<String, String> env = System.getenv();
            java.lang.reflect.Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            java.util.Map<String, String> writableEnv = (java.util.Map<String, String>) field.get(env);
            if (value == null) {
                writableEnv.remove(key);
            } else {
                writableEnv.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to set env var", e);
        }
    }

    private static void removeEnv(String key) {
        setEnv(key, null);
    }
}


class MakeProxyUrlTest {

    @Test
    void postgresqlUrl() {
        assertEquals(
            "postgresql://user:pass@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:pass@dbhost:5432/mydb", 7932)
        );
    }

    @Test
    void postgresUrl() {
        assertEquals(
            "postgres://user:pass@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgres://user:pass@remote.aws.com:5432/mydb", 7932)
        );
    }

    @Test
    void pgUrlWithoutPort() {
        assertEquals(
            "postgresql://user:pass@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:pass@host.aws.com/mydb", 7932)
        );
    }

    @Test
    void pgUrlWithoutPortOrPath() {
        assertEquals(
            "postgresql://user:pass@localhost:7932",
            GoldLapel.makeProxyUrl("postgresql://user:pass@host.aws.com", 7932)
        );
    }

    @Test
    void bareHostPort() {
        assertEquals("localhost:7932", GoldLapel.makeProxyUrl("dbhost:5432", 7932));
    }

    @Test
    void bareHost() {
        assertEquals("localhost:7932", GoldLapel.makeProxyUrl("dbhost", 7932));
    }

    @Test
    void preservesQueryParams() {
        assertEquals(
            "postgresql://user:pass@localhost:7932/mydb?sslmode=require",
            GoldLapel.makeProxyUrl("postgresql://user:pass@remote:5432/mydb?sslmode=require", 7932)
        );
    }

    @Test
    void preservesPercentEncodedPassword() {
        assertEquals(
            "postgresql://user:p%40ss@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:p%40ss@remote:5432/mydb", 7932)
        );
    }

    @Test
    void noUserinfo() {
        assertEquals(
            "postgresql://localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://dbhost:5432/mydb", 7932)
        );
    }

    @Test
    void noUserinfoNoPort() {
        assertEquals(
            "postgresql://localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://dbhost/mydb", 7932)
        );
    }

    @Test
    void localhostStaysLocalhost() {
        assertEquals(
            "postgresql://user:pass@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:pass@localhost:5432/mydb", 7932)
        );
    }

    @Test
    void atSignInPasswordWithPort() {
        assertEquals(
            "postgresql://user:p@ss@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:p@ss@host:5432/mydb", 7932)
        );
    }

    @Test
    void atSignInPasswordWithoutPort() {
        assertEquals(
            "postgresql://user:p@ss@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:p@ss@host/mydb", 7932)
        );
    }

    @Test
    void atSignInPasswordWithQueryParams() {
        assertEquals(
            "postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue",
            GoldLapel.makeProxyUrl("postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue", 7932)
        );
    }
}


class WaitForPortTest {

    @Test
    void openPortReturnsTrue() throws IOException {
        ServerSocket server = new ServerSocket(0);
        int port = server.getLocalPort();
        try {
            assertTrue(GoldLapel.waitForPort("127.0.0.1", port, 1000));
        } finally {
            server.close();
        }
    }

    @Test
    void closedPortTimesOutReturnsFalse() {
        assertFalse(GoldLapel.waitForPort("127.0.0.1", 19999, 200));
    }
}


class GoldLapelClassTest {

    @Test
    void defaultPort() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        assertEquals(7932, gl.getPort());
    }

    @Test
    void customPort() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb",
            new GoldLapel.Options().port(9000));
        assertEquals(9000, gl.getPort());
    }

    @Test
    void notRunningInitially() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }

    @Test
    void stopIsNoOpWhenNotStarted() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        gl.stopProxy();
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }

    @Test
    void stopIsIdempotent() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        gl.stopProxy();
        gl.stopProxy();
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }
}


class ModuleFunctionsTest {

    @Test
    void proxyUrlNullWhenNotStarted() {
        GoldLapel.stop();
        assertNull(GoldLapel.proxyUrl());
    }
}
