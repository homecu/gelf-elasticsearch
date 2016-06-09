gelf-elasticsearch
========

A trivial asyncio gelf server to elasticsearch relay


Usage
--------
```
python3.5 ./gelf_elasticsearch.py https://elasticsearch.location.foo:12345/your_index/your_type
```

Then you can direct gelf clients to `udp://localhost:12201` a la..
```
docker run --log-driver gelf --log-opt gelf-address=udp://localhost:12201 busybox date
```
