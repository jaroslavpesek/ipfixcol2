protobuf-kafka (output plugin)
==============================

The plugin serialises IPFIX flow records into Protocol Buffers and publishes
them to a Kafka topic. The ``.proto`` schema is loaded at runtime — no
compile-time code generation required. Field mappings between IPFIX information
elements and protobuf fields are declared in the IPFIXcol2 XML configuration.

Key Features
------------

- **Runtime Schema Loading** — The ``.proto`` file is parsed at startup via the
  protobuf descriptor API. No ``protoc`` code generation step is needed, which
  means the schema can be updated without recompiling the plugin.

- **Zero-Copy Wire Encoding** — Records are encoded directly into the protobuf
  wire format without constructing intermediate ``Message`` objects. Encoding
  buffers are reused across records to minimise allocations.

- **Flexible Field Mapping** — Any IPFIX/NetFlow information element can be
  mapped to any compatible protobuf field, including ``repeated`` fields backed
  by basicList IPFIX elements. The Observation Domain ID (ODID) can be included
  as a field via the special ``odid`` keyword.

- **RSS Partitioning** — Optionally pins records to Kafka partitions by
  ``flowId`` (modulo partition count), with a symmetric 5-tuple hash fallback.
  Both directions of a flow land on the same partition, enabling per-flow
  ordering on the consumer side.

How to build
------------

By default, the plugin is not distributed with IPFIXcol due to extra
dependencies. The following libraries (and their header files) must be installed
on your system before building:

- librdkafka ≥ 0.9.3
- protobuf (with headers)
- xxHash
- IPFIXcol2 (with headers)

Once the dependencies are in place, compile and install the plugin:

::

    $ mkdir build && cd build && cmake ..
    $ make
    # make install

Example .proto file
-------------------

.. code-block:: protobuf

    syntax = "proto3";
    package example;

    message FlowRecord {
        bytes   src_ip   = 1;
        bytes   dst_ip   = 2;
        uint32  src_port = 3;
        uint32  dst_port = 4;
        uint32  protocol = 5;
        uint64  bytes    = 6;
        uint64  packets  = 7;
        uint32  odid     = 8;
    }

Example configuration
---------------------

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
                <!-- Special field: Observation Domain ID from the IPFIX message header -->
                <field ipfix="odid"                              proto="odid"     />

                <!-- Standard IPFIX fields identified by IANA name -->
                <field ipfix="iana:sourceIPv4Address"            proto="src_ip"   />
                <field ipfix="iana:destinationIPv4Address"       proto="dst_ip"   />
                <field ipfix="iana:sourceTransportPort"          proto="src_port" />
                <field ipfix="iana:destinationTransportPort"     proto="dst_port" />
                <field ipfix="iana:protocolIdentifier"           proto="protocol" />
                <field ipfix="iana:octetDeltaCount"              proto="bytes"    />
                <field ipfix="iana:packetDeltaCount"             proto="packets"  />
            </map>
        </params>
    </output>

Parameters
----------

:``brokers``:
    Comma-separated list of Kafka broker addresses in ``host:port`` form.
    Required.

:``topic``:
    Kafka topic to publish flow records to. Required.

:``partition``:
    Partitioning strategy. ``random`` lets Kafka assign the partition.
    ``rss`` pins each record to a partition determined by ``flowId`` modulo the
    partition count, with a symmetric 5-tuple hash as a fallback (see
    `RSS Partitioning`_ for details). [default: random]

:``batch_size``:
    Maximum number of messages in a single batch before it is flushed. Maps to
    the ``batch.num.messages`` librdkafka property. [default: 10000]

:``linger_ms``:
    Maximum number of milliseconds to wait before flushing an incomplete batch.
    Maps to the ``queue.buffering.max.ms`` librdkafka property. [default: 100]

:``compression``:
    Compression codec applied to message batches. Accepted values: ``none``,
    ``gzip``, ``snappy``, ``lz4``, ``zstd``. [default: lz4]

:``blocking``:
    When true, the processing thread blocks if the producer queue is full
    instead of dropping records. [default: false]

:``proto_file``:
    Absolute path to the ``.proto`` file describing the message schema.
    Required.

:``message_type``:
    Fully qualified protobuf message type, including the package prefix
    (e.g. ``example.FlowRecord``). Required.

:``map``:
    Container for one or more ``<field>`` elements that declare the mapping
    between IPFIX information elements and protobuf fields. At least one
    ``<field>`` element is required.

Field mappings
--------------

Each ``<field>`` element inside ``<map>`` carries two attributes:

- ``ipfix`` — Identifies the IPFIX source. Accepted formats:

  - ``scope:name`` — Element identified by a scope and name
    (e.g. ``iana:sourceIPv4Address``).
  - ``e<pen>id<id>`` — Element identified numerically by Private Enterprise
    Number and field ID (e.g. ``e0id8``).
  - ``<root>/<list_elem>`` — Selects an element inside a basicList
    (e.g. ``e0id291/cesnet:packetLength``). The target protobuf field must be
    declared as ``repeated``.
  - ``odid`` — Special keyword (case-insensitive). The value is the Observation
    Domain ID taken from the IPFIX message header, not from the record data.
    Recommended target type: ``uint32``.

- ``proto`` — Name of the target protobuf field within the configured
  ``message_type``.

The following table lists the IPFIX abstract data types and the protobuf field
types that are compatible with each of them:

.. list-table:: Mapping of IPFIX abstract data types to protobuf field types
    :header-rows: 1

    * - **IPFIX Abstract Data Type**
      - **Compatible Protobuf Field Types**
    * - unsigned8 / unsigned16 / unsigned32
      - uint32, uint64, int32, int64, sint32, sint64, fixed32, fixed64, bool, enum
    * - unsigned64
      - uint64, int64, sint64, fixed64
    * - signed8 / signed16 / signed32
      - int32, int64, sint32, sint64, sfixed32, sfixed64
    * - signed64
      - int64, sint64, sfixed64
    * - float32
      - float
    * - float64
      - double
    * - ipv4Address / ipv6Address
      - bytes (little-endian byte order), string
    * - string
      - string (invalid UTF-8 bytes are replaced with ``?``)
    * - octetArray
      - bytes
    * - dateTimeSeconds
      - uint32, int32 (value is Unix seconds)
    * - dateTimeMilliseconds / dateTimeMicroseconds / dateTimeNanoseconds
      - uint64, int64 (value is milliseconds since the Unix epoch)

RSS Partitioning
----------------

When ``partition=rss``, the plugin attempts to assign each record to a Kafka
partition deterministically. The primary key is ``flowId`` (IANA IE 148): the
partition is computed as ``flowId % partition_count``.

If ``flowId`` is not present in the record, the plugin falls back to a
symmetric 5-tuple hash. The source and destination IP addresses, source and
destination transport ports, and the protocol identifier are XOR-combined and
then hashed with XXH64. Because the XOR is commutative, the forward and reverse
directions of a flow produce an identical hash value and therefore land on the
same partition.

The partition count is queried from Kafka metadata at plugin startup. If the
metadata is unavailable, a count of 1 is assumed, which causes all records to
be written to partition 0.

Performance tips
----------------

- Use ``lz4`` compression (the default) for the best CPU-to-compression-ratio
  tradeoff. Switch to ``zstd`` if storage cost is the primary concern.
- Increase ``batch_size`` and ``linger_ms`` to improve throughput when a small
  amount of added latency is acceptable.
- Use ``rss`` only when consumers require per-flow ordering. The ``random``
  strategy has no hashing overhead and should be preferred in
  throughput-oriented deployments.
