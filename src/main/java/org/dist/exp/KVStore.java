package org.dist.exp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//The file can be read on every restart and the state can be recovered by replaying all the log entries.   Consider a simple in memory key-value store:
public class KVStore {

    private final WriteAheadLog wal;
    private final Config config;
    private Map<String, String> kv = new HashMap<>();

    public KVStore(Config config) {
        this.config = config;
        this.wal = WriteAheadLog.openWAL(config);
        this.applyLog();
    }

    public void applyLog() {
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
    }

    private void applyEntries(List<WALEntry> walEntries) {
        for (WALEntry walEntry : walEntries) {
            Command command = deserialize(walEntry);
            if (command instanceof SetValueCommand) {
                SetValueCommand setValueCommand = (SetValueCommand)command;
                kv.put(setValueCommand.key, setValueCommand.value);
            }
        }
    }

    private Command deserialize(WALEntry walEntry) {
        return null;
    }


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

    public void initialiseFromSnapshot(SnapShot snapShot) {
        kv.putAll(snapShot.deserializeState());
    }

}
