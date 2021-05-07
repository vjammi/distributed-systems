package org.dist.exp;

public class WALEntry {

    private final Long entryId;
    private final byte[] data;
    private final EntryType entryType;
    private long timeStamp;

    public WALEntry(Long entryId, byte[] data, EntryType entryType, long timeStamp) {
        this.entryId = entryId;
        this.data = data;
        this.entryType = entryType;
        this.timeStamp = timeStamp;
    }

    class EntryType {
    }
}
