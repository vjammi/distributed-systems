package org.dist.exp;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class Command {

    protected static int setValueType() {
        return 0;
    }

    public abstract void serialize(DataOutputStream os) throws IOException;

    public class SetValueType {
    }
}
