package org.dist.utils;

import java.io.File;
import java.util.Random;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JTestUtil {

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

    public static File tmpDir2(String prefix) {
        String ioDir = System.getProperty("java.io.tmpdir");
        File f = new File(ioDir, prefix + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }

    // https://github.com/kafka-dev/kafka/blob/master/core/src/test/scala/unit/kafka/utils/TestUtils.scala
    public static File tmpDir(String testWal) {
        String ioDir = System.getProperty("java.io.tmpdir");
        Random random = new Random();
        File f = new File(ioDir, "wal-" + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }


}
