from google.protobuf import json_format
import cfw_pb2


jsonmsg = '{"timestamp": "2019-05-21T14:24", "event_type": "flow-event", "action": "allow", "src_ip": "1.1.1.1", "dst_ip": "10.1.1.1", "nat_src_ip": "172.168.32.23", "nat_src_port": 100, "session_id": 45454, "src_port": 104, "dst_port": 4063, "protocol": "http", "app_id": "facebook", "rule_name": "facebook_rule", "user_name": "aniket", "repeat_count": 9, "bytes_sent": 3836, "bytes_rcvd": 4625, "packet_sent": 32, "packet_rcvd": 6, "start_time": "2019-05-21T14:24", "session_duration": 41, "tunnel_type": "ipsec", "tenant_id": "vw", "dst_ip_location": {"AccuracyRadius": 90, "Latitude": 17, "Longitude": 97, "MetroCode": 83, "TimeZone": "IST"}, "src_ip_location": {"AccuracyRadius": 13, "Latitude": 44, "Longitude": 86, "MetroCode": 11, "TimeZone": "IST"}}'

rawbytes = json_format.Parse(jsonmsg, cfw_pb2.cfe(), ignore_unknown_fields=False)

print(rawbytes)