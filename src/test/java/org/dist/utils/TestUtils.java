package org.dist.utils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.concurrent.Callable;
import static org.junit.Assert.fail;

public class TestUtils {
    public static List<Integer> choosePorts(Integer count) throws IOException {
        List<ServerSocket> serverSockets = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            serverSockets.add(new ServerSocket(0));
        }
        List<Integer> ports = serverSockets.stream().map(s -> {
            int localPort = s.getLocalPort();
            try {
                s.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return localPort;
        }).collect(Collectors.toList());

        return ports;
    }

    public static Integer choosePort() throws IOException {
        return choosePorts(1).get(0);
    }

    static Random random = new Random();

    public static File tempDir(String prefix) {
        String ioDir = System.getProperty("java.io.tmpdir");
        File f = new File(ioDir, prefix + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }

    /**
     * Wait until a callable returns true or the timeout is reached.
     */
    public static void waitUntilTrue(Callable<Boolean> callable, long timeoutMs, String errorMsg) {
        try {
            long startTime = System.currentTimeMillis();
            Boolean state = false;
            do {
                state = callable.call();
                if (System.currentTimeMillis() > startTime + timeoutMs) {
                    fail(errorMsg);
                }
                Thread.sleep(50);
            } while (!state);
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
    }
}
