
package com.meituan.ptubes.reader.producer.mysqlreplicator.common.packet;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import com.meituan.ptubes.reader.producer.mysqlreplicator.common.column.StringColumn;
import com.meituan.ptubes.reader.producer.mysqlreplicator.network.net.Packet;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.ToStringBuilder;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XDeserializer;
import com.meituan.ptubes.reader.producer.mysqlreplicator.utils.XSerializer;

public class ResultSetRowPacket extends AbstractPacket {
	private static final long serialVersionUID = 698187140476020984L;

	private List<StringColumn> columns;

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("columns", columns).toString();
	}

	@Override public byte[] getPacketBody() {
		final XSerializer s = new XSerializer(1024);
		for (StringColumn column : this.columns) {
			s.writeLengthCodedString(column);
		}
		return s.toByteArray();
	}

	public List<StringColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<StringColumn> columns) {
		this.columns = columns;
	}

	public static ResultSetRowPacket valueOf(Packet packet) throws IOException {
		final XDeserializer d = new XDeserializer(packet.getPacketBody());
		final ResultSetRowPacket r = new ResultSetRowPacket();
		r.length = packet.getLength();
		r.sequence = packet.getSequence();
		r.setColumns(new LinkedList<>());
		while (d.available() > 0) {
			r.getColumns().add(d.readLengthCodedString());
		}
		return r;
	}
}
