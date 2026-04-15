==================================
ipfixcol2-protobuf-kafka-output
==================================

:Author: Jaroslav Pesek
:Date: 2026
:Manual section: 7

DESCRIPTION
===========

The **protobuf-kafka** plugin serializes IPFIX flow records into Protocol
Buffers and sends them to a Kafka topic. It features:

- **Dynamic Schema Loading**: Load .proto files at runtime without
  compile-time code generation
- **Reflection-Free Encoding**: Direct protobuf wire encoding on the hot path
  with reusable buffers
- **RSS Partitioning**: flowId-based partitioning (with 5-tuple fallback)
  for receiver-side scaling
- **Kafka Batching**: Aggressive batching to reduce network interrupts

CONFIGURATION
=============

The plugin accepts the following XML configuration:

.. code-block:: xml

    <output>
        <name>Protobuf Kafka output</name>
        <plugin>protobuf-kafka</plugin>
        <params>
            <brokers>kafka.example.com:9092</brokers>
            <topic>flows</topic>
            <partition>rss</partition>
            <batch_size>10000</batch_size>
            <linger_ms>100</linger_ms>
            <compression>lz4</compression>
            <proto_file>/etc/ipfixcol2/schemas/flow.proto</proto_file>
            <message_type>example.FlowRecord</message_type>
            <map>
                <field ipfix="odid"                          proto="odid"     />
                <field ipfix="iana:sourceIPv4Address"        proto="src_ip"   />
                <field ipfix="iana:destinationIPv4Address"   proto="dst_ip"   />
                <field ipfix="iana:sourceTransportPort"      proto="src_port" />
                <field ipfix="iana:destinationTransportPort" proto="dst_port" />
                <field ipfix="iana:protocolIdentifier"       proto="protocol" />
                <field ipfix="iana:octetDeltaCount"          proto="bytes"    />
                <field ipfix="iana:packetDeltaCount"         proto="packets"  />
            </map>
        </params>
    </output>

PARAMETERS
==========

brokers
    Comma-separated list of Kafka brokers (required)

topic
    Kafka topic to produce to (required)

partition
    Partition strategy: ``random`` (default) or ``rss`` (flowId, fallback to 5-tuple hash)

batch_size
    Kafka producer batch.num.messages (default: 10000)

linger_ms
    Kafka producer queue.buffering.max.ms (default: 100)

compression
    Kafka compression codec (default: lz4)

blocking
    Block when producer queue is full instead of dropping messages (default: false)

proto_file
    Path to .proto file defining the message schema (required)

message_type
    Fully qualified Protobuf message type name (required)

map
    Field mappings from IPFIX to Protobuf (at least one required)

FIELD MAPPINGS
==============

Each ``<field>`` element in ``<map>`` specifies:

ipfix
    IPFIX element specification in one of these formats:

    - ``scope:name`` (e.g., ``iana:sourceIPv4Address``)
    - ``e<pen>id<id>`` (e.g., ``e0id8``)
    - ``<root>/<list_elem>`` for basicList element selection
      (e.g., ``e0id291/cesnet:packetLength``)

    ``odid``
        Special keyword (case-insensitive). The Observation Domain ID from the
        IPFIX message header is written to the target protobuf field as a
        ``uint32`` value. The recommended protobuf field type is ``uint32``.

proto
    Protobuf field name in the target message

If ``ipfix`` uses ``<root>/<list_elem>``, ``proto`` must reference a
``repeated`` Protobuf field.

If an IPFIX IPv4/IPv6 field is mapped to Protobuf ``bytes``, payload bytes are
emitted in little-endian order (reversed from network byte order).

Supported IPFIX to Protobuf type mappings:

- Integer types (8/16/32/64-bit) → int32/int64/uint32/uint64
- IP addresses → bytes or string
- Strings → string
- Octet arrays → bytes

Example mappings for packet-level basicList fields:

.. code-block:: xml

    <map>
        <field ipfix="e0id291/cesnet:packetLength" proto="PPI_PKT_LENGTHS" />
        <field ipfix="e0id291/cesnet:packetTime" proto="PPI_PKT_TIMES" />
        <field ipfix="e0id291/cesnet:packetDirection" proto="PPI_PKT_DIRECTIONS" />
        <field ipfix="e0id291/cesnet:packetFlag" proto="PPI_PKT_FLAGS" />
    </map>

RSS PARTITIONING
================

When ``partition`` is set to ``rss``, the plugin uses ``flowId`` to pick
partition ``flowId % partition_count``. If ``flowId`` is missing, it falls
back to symmetric hashing from the 5-tuple (source IP, destination IP,
source port, destination port, protocol). This ensures:

- Both directions of a flow go to the same partition
- Load is distributed evenly across partitions
- Consumers can process flows in order per-connection

EXAMPLE PROTO FILE
==================

.. code-block:: protobuf

    syntax = "proto3";
    package Retina;

    message FlowRecord {
        bytes src_ip = 1;
        bytes dst_ip = 2;
        uint32 proto = 3;
        uint64 bytes = 4;
        uint64 packets = 5;
    }

SEE ALSO
========

ipfixcol2(1), ipfixcol2-json-kafka-output(7)
