package org.dist.bookkeeper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
//import org.dist.kvstore.InetAddressAndPort;
//import org.dist.queue.TestUtils;
import org.dist.utils.TestUtils;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
//import static org.junit.Assert.*;

public class JournalTest {

    @Test
    public void shouldWriteAndFlushJournalEntries() throws InterruptedException {
        int entrySize = 1024;
        long l1 = 1L;
        long l2 = 2L;
        Journal journal = new Journal(TestUtils.tempDir("journal"));
        journal.start();
        List<Long> entriedFlushedToDisk = new ArrayList<Long>();

        Journal.WriteCallback writeCallback = (int rc, long ledgerId, long entryId1, InetAddress addr, Object ctx) -> {
            entriedFlushedToDisk.add(entryId1);
        };
        for (int i = 0; i < 10000; i++) {
            int entryId = i + 1;

            journal.logAddEntry(1, entryId, createByteBuf(l1, 0L, entrySize), false, writeCallback, this);
        }

        Callable<Boolean> booleanCallable = () -> {
            return entriedFlushedToDisk.size() == 10000;
        };
        TestUtils.waitUntilTrue(booleanCallable, 10000, "Waiting for entries to be flushed to disk");

    }

    private static ByteBuf createByteBuf(long ledgerId, long entryId, int entrySize) {
        byte[] data = new byte[entrySize];
        ThreadLocalRandom.current().nextBytes(data);
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        buffer.writerIndex(0);
        buffer.writeLong(ledgerId);
        buffer.writeLong(entryId);
        buffer.writeLong(entryId - 1); // lac
        buffer.writerIndex(entrySize);
        return buffer;
    }
}