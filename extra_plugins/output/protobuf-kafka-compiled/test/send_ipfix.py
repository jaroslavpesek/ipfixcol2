#!/usr/bin/env python3
import socket
import struct
import time

UDP_IP = "127.0.0.1"
UDP_PORT = 4739

def build_ipfix_header(total_length, export_time, seq_number, odid):
    version = 10
    return struct.pack("!HHIII", version, total_length, export_time, seq_number, odid)

# Template Set
template_id = 256
template_fields = [
    (8, 4),   # sourceIPv4Address
    (12, 4),  # destinationIPv4Address
    (4, 1),   # protocolIdentifier
    (1, 8),   # octetDeltaCount
]
field_count = len(template_fields)

template_set_header = struct.pack("!HH", 2, 4 + 4 + field_count*4)  # Set ID=2 (Template), length
template_header = struct.pack("!HH", template_id, field_count)
template_fields_bytes = b"".join([struct.pack("!HH", fid, flen) for fid, flen in template_fields])
template_set = template_set_header + template_header + template_fields_bytes

# Data Sets
data_records = [
    ("192.168.0.1", "192.168.0.2", 6, 12345),
    ("10.0.0.1",    "10.0.0.2",   17, 6789),
    ("172.16.0.1",  "172.16.0.2", 1, 42),
]

data_sets = []
for src, dst, proto, bytes_count in data_records:
    src_ip = struct.pack("!BBBB", *map(int, src.split(".")))
    dst_ip = struct.pack("!BBBB", *map(int, dst.split(".")))

    protocol = struct.pack("!B", proto)
    bytes_field = struct.pack("!Q", bytes_count)
    record = src_ip + dst_ip + protocol + b"\x00" + bytes_field
    data_set_length = 4 + len(record)
    data_set_header = struct.pack("!HH", template_id, data_set_length)
    data_sets.append(data_set_header + record)

# Send over UDP
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
export_time = int(time.time())
seq_number = 1
odid = 1234

# 1) Send Template + first Data Set together (seq=1)
packet = build_ipfix_header(16 + len(template_set + data_sets[0]), export_time, seq_number, odid)
sock.sendto(packet + template_set + data_sets[0], (UDP_IP, UDP_PORT))
print(f"Sent Template + first Data Set (seq={seq_number})")

# 2) Send remaining Data Sets with incremented sequence numbers
for i, ds in enumerate(data_sets[1:], start=1):
    seq_number += 1
    packet = build_ipfix_header(16 + len(ds), export_time, seq_number, odid)
    sock.sendto(packet + ds, (UDP_IP, UDP_PORT))
    print(f"Sent Data Set {i+1} (seq={seq_number})")
