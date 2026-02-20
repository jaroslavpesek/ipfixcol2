from kafka import KafkaConsumer
import schema_pb2 # Schema generated for python by command protoc --python_out=. schema

consumer = KafkaConsumer(
    'IPFlows',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda b: b
)

for msg in consumer:
    record = schema_pb2.FlowRecord()
    record.ParseFromString(msg.value)
    print(f"SRC_IP: {record.SRC_IP}, DST_IP: {record.DST_IP}, PROTOCOL: {record.PROTOCOL}, BYTES: {record.BYTES}")

