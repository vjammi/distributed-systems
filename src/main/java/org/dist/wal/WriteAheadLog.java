package org.dist.wal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteAheadLog {

    private static String logSuffix = ".log";
    private static String logPrefix = "wal";
    private static int firstLogId = 0;
    static int sizeOfInt = 4;
    static int sizeOfLong = 8;
    final RandomAccessFile randomAccessFile;
    final FileChannel fileChannel;


    private WriteAheadLog(File file) {
        try {
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();

        } catch (FileNotFoundException e) {

            throw new RuntimeException(e);
        }
    }

    public static WriteAheadLog openWAL(Integer startIndex, File walDir) {
        File file = new File(walDir, createFileName(startIndex));
        return new WriteAheadLog(file);
    }

    private static String createFileName(Integer startIndex) {
        return logPrefix + "_" + startIndex + logSuffix;
    }

    public void write(String s) {
        writeEntry(s.getBytes());
    }

    private Long lastLogEntryId = 0l;
    private Map<Long, Long> entryOffsets = new HashMap<Long, Long>();

    public Long writeEntry(byte[] bytes) {
        long logEntryId = lastLogEntryId + 1;
        WriteAheadLogEntry logEntry = new WriteAheadLogEntry(logEntryId, bytes, 0);
        Long filePosition = writeEntry(logEntry);
        lastLogEntryId = logEntryId;
        entryOffsets.put(logEntryId, filePosition);
        return logEntryId;
    }

    public List<WriteAheadLogEntry> readAll() {
        try {
            fileChannel.position(0);
            long totalBytesRead = 0L;
            ArrayList<WriteAheadLogEntry> entries = new ArrayList<WriteAheadLogEntry>();
            WALEntryDeserializer deserializer = new WALEntryDeserializer(fileChannel);
            while (totalBytesRead < fileChannel.size()) {
                long startPosition = fileChannel.position();
                WriteAheadLogEntry entry = deserializer.readEntry();
                totalBytesRead += entry.entrySize() + WriteAheadLog.sizeOfInt; //size of entry + size of int which stores length
                entryOffsets.put(entry.getEntryId(), startPosition);
                entries.add(entry);
            }
            return entries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Long writeEntry(WriteAheadLogEntry entry) {
        ByteBuffer buffer = entry.serialize();
        return writeToChannel(buffer);
    }

    private Long writeToChannel(ByteBuffer buffer) {
        try {
            buffer.flip();
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
            fileChannel.force(false);
            return fileChannel.position();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        try {
            fileChannel.force(true);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        flush();

        try {
            fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void truncate(Long logIndex) {
        Long filePosition = entryOffsets.get(logIndex);
        if (filePosition == null) throw new IllegalArgumentException("No file position available for logIndex=" + logIndex);

        try {
            fileChannel.truncate(filePosition);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
