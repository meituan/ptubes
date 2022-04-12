package com.meituan.ptubes.reader.container.common.vo;

import java.util.List;
import java.util.Optional;

public enum PtubesFieldType {

    // Contains all AvroPrimitiveTypes._avroType
    INT("int"),
    LONG("long"),
    STRING("string"),
    BYTES("bytes"),
    FLOAT("float"),
    DOUBLE("double"),
    BOOLEAN("boolean"),  // In the future, support for more types of fragmented value calculations can be added here
    DATE("date"),
    TIME("time"),
    TIMESTAMP("timestamp"),
    DATETIME("datetime"),
    GEOMETRY("geometry"),
    OTHERS("");

    private String type;

    PtubesFieldType(String type) {
        this.type = type;
    }

    public static Optional<PtubesFieldType> getSchemaType(List<String> types) {
        for (PtubesFieldType ptubesFieldType : PtubesFieldType.values()) {
            if (types.contains(ptubesFieldType.type)) {
                return Optional.of(ptubesFieldType);
            }
        }
        return Optional.empty();
    }

}
