package com.goldlapel.rxjava3;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.reactivex.rxjava3.disposables.Disposable;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that disposing the {@code Single<RxJavaGoldLapel>} returned by
 * {@link RxJavaGoldLapel#start(String)} kills the spawned proxy subprocess
 * — no leaks. Mirrors the reactor module's ReactiveCancellationTest.
 *
 * <p>Uses a fake binary (shell script) that binds the requested port but
 * speaks no Postgres protocol. The sync {@code GoldLapel.start()} blocks
 * on eagerConnect(); subscribing via RxJava and cancelling mid-wait
 * should propagate to {@code sink.onCancel} → {@code gl.stop()} → kill.
 */
class RxJavaCancellationTest {

    @Test
    void cancellingStartKillsSpawnedSubprocess(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(
            !System.getProperty("os.name", "").toLowerCase().contains("windows"),
            "POSIX-only test (needs /bin/sh + python3)"
        );
        Assumptions.assumeTrue(
            isOnPath("python3"),
            "python3 not on PATH"
        );

        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }

        Path pidFile = tmp.resolve("fake.pid");
        Path script = writeFakeProxyBinary(tmp, pidFile);

        String origBin = System.getenv("GOLDLAPEL_BINARY");
        try {
            setEnvReflective("GOLDLAPEL_BINARY", script.toString());

            // Start, then cancel. The sync GoldLapel.start() path eagerConnects
            // via JDBC — our fake server never answers the handshake, so start
            // is still blocking when we dispose. Cancellation must reach it.
            AtomicReference<Throwable> asyncErr = new AtomicReference<>();
            Disposable d = RxJavaGoldLapel.start(
                "postgresql://localhost:5432/mydb",
                opts -> opts.setProxyPort(port)
            ).subscribe(
                ignored -> { /* won't fire — we cancel first */ },
                asyncErr::set
            );

            // Give the spawn a moment to record its PID
            long deadline = System.nanoTime() + 3_000_000_000L;
            while (System.nanoTime() < deadline && !Files.exists(pidFile)) {
                Thread.sleep(25);
            }
            // Cancel regardless of whether we beat spawn or not — both
            // paths (cancel-before-success + cancel-after-spawn) should
            // clean up.
            d.dispose();

            // PID should be recorded (fake binary writes $$ before binding).
            assertTrue(Files.exists(pidFile),
                "fake binary should have recorded its PID before cancel");
            long pid = Long.parseLong(Files.readString(pidFile).trim());

            waitForProcessExit(pid, 10_000);
            assertFalse(
                ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false),
                "subprocess PID " + pid + " should have been killed on cancellation — leak!"
            );
        } finally {
            setEnvReflective("GOLDLAPEL_BINARY", origBin);
        }
    }

    // ─── helpers (mirrors FactoryApiTest) ──────────────────────

    private static Path writeFakeProxyBinary(Path tmp, Path pidFile) throws IOException {
        Path script = tmp.resolve("fake-goldlapel.sh");
        Files.writeString(script,
            "#!/bin/sh\n" +
            "echo $$ > \"" + pidFile.toString() + "\"\n" +
            "PORT=\n" +
            "while [ $# -gt 0 ]; do\n" +
            "  case \"$1\" in\n" +
            "    --proxy-port) PORT=\"$2\"; shift 2 ;;\n" +
            "    *) shift ;;\n" +
            "  esac\n" +
            "done\n" +
            "exec python3 -c \"\n" +
            "import socket, time\n" +
            "s = socket.socket()\n" +
            "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n" +
            "s.bind(('127.0.0.1', ${PORT}))\n" +
            "s.listen(5)\n" +
            "while True:\n" +
            "    try:\n" +
            "        c, _ = s.accept()\n" +
            "        # Keep the connection open so JDBC eagerConnect blocks;\n" +
            "        # this gives the test a chance to cancel mid-wait.\n" +
            "        time.sleep(30)\n" +
            "        c.close()\n" +
            "    except Exception:\n" +
            "        break\n" +
            "\"\n"
        );
        script.toFile().setExecutable(true);
        return script;
    }

    private static boolean isOnPath(String binary) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) return false;
        for (String dir : pathEnv.split(":")) {
            if (new java.io.File(dir, binary).canExecute()) return true;
        }
        return false;
    }

    private static void waitForProcessExit(long pid, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            boolean alive = ProcessHandle.of(pid)
                .map(ProcessHandle::isAlive).orElse(false);
            if (!alive) return;
            try { Thread.sleep(50); } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void setEnvReflective(String key, String value) {
        try {
            java.util.Map<String, String> env = System.getenv();
            java.lang.reflect.Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            java.util.Map<String, String> writable = (java.util.Map<String, String>) field.get(env);
            if (value == null) writable.remove(key);
            else writable.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set env var", e);
        }
    }
}
