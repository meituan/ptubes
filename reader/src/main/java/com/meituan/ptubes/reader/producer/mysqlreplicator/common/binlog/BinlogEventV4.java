package com.meituan.ptubes.reader.producer.mysqlreplicator.common.binlog;

/**
 * +=====================================+
 * | event  | timestamp         0 : 4    |
 * | header +----------------------------+
 * |        | type_code         4 : 1    |
 * |        +----------------------------+
 * |        | server_id         5 : 4    |
 * |        +----------------------------+
 * |        | event_length      9 : 4    |
 * |        +----------------------------+
 * |        | next_position    13 : 4    |
 * |        +----------------------------+
 * |        | flags            17 : 2    |
 * +=====================================+
 * | event  | fixed part       19 : y    |
 * | data   +----------------------------+
 * |        | variable part              |
 * +=====================================+
 */
public interface BinlogEventV4 {

	BinlogEventV4Header getHeader();
}
