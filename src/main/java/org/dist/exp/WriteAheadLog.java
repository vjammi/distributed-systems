package org.dist.exp;

import java.util.List;

public class WriteAheadLog {


    public static WriteAheadLog openWAL(Config config) {
        return null;
    }

    public List<WALEntry> readAll() {
        return null;
    }

    public void writeEntry(){

    }

    public Long writeEntry(Object serialize) {
        return null;
    }

    /*
    public String get(String key) {
        return kv.get(key);
    }

    public void put(String key, String value) {
        appendLog(key, value);
        kv.put(key, value);
    }

    private Long appendLog(String key, String value) {
        return wal.writeEntry(new SetValueCommand(key, value).serialize());
    }
    */
}
