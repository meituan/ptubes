package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.UnsignedLong;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Packet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XDeserializer;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XSerializer;

// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
public class ColumnDefPacket extends AbstractPacket {

    private boolean isComFieldList;
    private StringColumn catalog;
    private StringColumn schema;
    private StringColumn table;
    private StringColumn orgTable;
    private StringColumn name;
    private StringColumn orgName;
    private UnsignedLong fixedLength;
    private byte[] fixedLengthFields;
    // COM_FIELD_LIST
    private UnsignedLong defaultValuesLen;
    private List<StringColumn> defaultValues;

    @Override public byte[] getPacketBody() throws IOException {
        final XSerializer s = new XSerializer(1024);
        s.writeLengthCodedString(catalog);
        s.writeLengthCodedString(schema);
        s.writeLengthCodedString(table);
        s.writeLengthCodedString(orgTable);
        s.writeLengthCodedString(name);
        s.writeLengthCodedString(orgName);
        s.writeUnsignedLong(fixedLength);
        s.writeBytes(fixedLengthFields);
        if (isComFieldList) {
            s.writeUnsignedLong(defaultValuesLen);
            for (StringColumn sc : defaultValues) {
                s.writeLengthCodedString(sc);
            }
        }
        return s.toByteArray();
    }

    public boolean isComFieldList() {
        return isComFieldList;
    }

    public StringColumn getCatalog() {
        return catalog;
    }

    public StringColumn getSchema() {
        return schema;
    }

    public StringColumn getTable() {
        return table;
    }

    public StringColumn getOrgTable() {
        return orgTable;
    }

    public StringColumn getName() {
        return name;
    }

    public StringColumn getOrgName() {
        return orgName;
    }

    public UnsignedLong getFixedLength() {
        return fixedLength;
    }

    public byte[] getFixedLengthFields() {
        return fixedLengthFields;
    }

    public UnsignedLong getDefaultValuesLen() {
        return defaultValuesLen;
    }

    public List<StringColumn> getDefaultValues() {
        return defaultValues;
    }

    public static ColumnDefPacket valueOf(boolean isComFiledList, Packet packet) throws IOException {
        final XDeserializer d = new XDeserializer(packet.getPacketBody());
        final ColumnDefPacket r = new ColumnDefPacket();
        r.isComFieldList = isComFiledList;
        r.length = packet.getLength();
        r.sequence = packet.getSequence();
        r.catalog = d.readLengthCodedString();
        r.schema = d.readLengthCodedString();
        r.table = d.readLengthCodedString();
        r.orgTable = d.readLengthCodedString();
        r.name = d.readLengthCodedString();
        r.orgName = d.readLengthCodedString();
        r.fixedLength = d.readUnsignedLong();
        r.fixedLengthFields = d.readBytes(r.fixedLength.intValue());
        //
        if (isComFiledList) {
            r.defaultValuesLen = d.readUnsignedLong();
            r.defaultValues = new ArrayList<>();
            for (int i = 0, len = r.defaultValuesLen.intValue(); i < len; ++i) {
                r.defaultValues.add(d.readLengthCodedString());
            }
        }
        return r;
    }
}
