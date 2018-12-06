package org.apache.avro;

import java.io.Serializable;

public class SchemaImpl extends Schema implements Serializable {
    SchemaImpl(Type type) {
        super(type);
    }
}
