import socket

hostname, _, ips = socket.gethostbyname_ex(socket.gethostname())
INTERNAL_IPS = ['127.0.0.1'] + [ip[:-1] + '1' for ip in ips]
