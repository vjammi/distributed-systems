package org.dist.exp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class SetValueCommand extends Command {
    final String key;
    final String value;
    final String attachLease;
    public SetValueCommand(String key, String value) {
        this(key, value, "");
    }
    public SetValueCommand(String key, String value, String attachLease) {
        this.key = key;
        this.value = value;
        this.attachLease = attachLease;
    }

    @Override
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(Command.setValueType());
        os.writeUTF(key);
        os.writeUTF(value);
        os.writeUTF(attachLease);
    }

    public static SetValueCommand deserialize(InputStream is) {
        try {
            DataInputStream dataInputStream = new DataInputStream(is);
            return new SetValueCommand(dataInputStream.readUTF(), dataInputStream.readUTF(), dataInputStream.readUTF());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Object serialize() {
        return null;
    }
}
