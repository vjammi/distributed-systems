package org.dist.wal;

import java.nio.ByteBuffer;

class WriteAheadLogEntry {
    private final Long entryId;
    private final byte[] data;
    private final Integer entryType;

    public WriteAheadLogEntry(Long entryId, byte[] data, Integer entryType) {
        this.entryId = entryId;
        this.data = data;
        this.entryType = entryType;
    }

    public Long getEntryId() {
        return entryId;
    }

    public byte[] getData() {
        return data;
    }

    public Integer getEntryType() {
        return entryType;
    }

    public ByteBuffer serialize() {
        Integer entrySize = entrySize();
        int bufferSize = entrySize + 4; //4 bytes for record length + walEntry size
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putInt(entrySize);
        buffer.putInt(0); //normal entry
        buffer.putLong(entryId);
        buffer.put(data);
        return buffer;
    }

    Integer entrySize() {
        return data.length + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfInt; //size of all the fields
    }
}