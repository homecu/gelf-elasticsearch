#!/usr/bin/env python3

import aiohttp
import asyncio
import gzip
import json
import sys

loop = asyncio.get_event_loop()
es = aiohttp.ClientSession(loop=loop)


class GelfServerProtocol(object):

    def datagram_received(self, data, addr):
        log = json.loads(gzip.decompress(data).decode())
        asyncio.ensure_future(self.relaylog(log))

    async def relaylog(self, log):
        with aiohttp.Timeout(60):
            log['timestamp'] = round(log['timestamp'] * 1000)
            async with es.post(sys.argv[1], data=json.dumps(log)) as response:
                print(await response.text())


listen = loop.create_datagram_endpoint(GelfServerProtocol,
                                       local_addr=('0.0.0.0', 12201))
transport, protocol = loop.run_until_complete(listen)

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

transport.close()
loop.close()
