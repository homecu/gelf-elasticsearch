#!/usr/bin/env python3

import aiohttp
import asyncio
import datetime
import gzip
import json
import logging.handlers
import re
import shellish

_prio_names = logging.handlers.SysLogHandler.priority_names
log_level_map = dict(map(reversed, _prio_names.items()))
loop = asyncio.get_event_loop()


class GelfServerProtocol(object):

    image_re = re.compile('((?P<repo>.*?)/)?(?P<tag>[^:]*)(:(?P<version>.*))?')

    def connection_made(self, transport):
        pass

    def datagram_received(self, data, addr):
        log = json.loads(gzip.decompress(data).decode())
        dt = datetime.datetime.utcfromtimestamp(log['timestamp'])
        image_info = self.image_re.match(log['_image_name']).groupdict()
        record = {
            "command": log['_command'],
            "container_created": log['_created'],
            "container_id": log['_container_id'],
            "container_name": log['_container_name'],
            "host": self.instance_id,
            "host_addr": self.instance_ip,
            "image_id": log['_image_id'],
            "image_name": log['_image_name'],
            "image_repo": image_info['repo'] or '',
            "image_tag": image_info['tag'],
            "image_version": image_info['version'] or 'latest',
            "level": log_level_map[log['level']],
            "message": log['short_message'],
            "tag": log['_tag'],
            "timestamp": dt.isoformat(),
        }
        if self.verbose:
            shellish.vtmlprint('<b>LOG RECORD:<b>', record)
        asyncio.ensure_future(self.relaylog(record))

    async def relaylog(self, log):
        data = json.dumps(log)
        try:
            with aiohttp.Timeout(60):
                async with self.es_session.post(self.es_url, data=data) as r:
                    if r.status != 201:
                        shellish.vtmlprint('<b><red>ES POST ERROR:</red> %s</b>' %
                                           (await r.text()))
                    elif self.verbose:
                        shellish.vtmlprint('<b>ES INDEX:</b>', await r.text())
        except asyncio.TimeoutError:
            shellish.vtmlprint('<b><red>ES POST TIMEOUT</red></b>')


@shellish.autocommand
def gelf_es_relay(elasticsearch_url, listen_addr='0.0.0.0', listen_port=12201,
                  verbose=False, es_conn_limit=100, instance_id=None,
                  instance_ip=None):
    """ A Gelf server relay to elasticsearch.

    The URL should contain the /index/type args as per the elasticsearch API.

    E.g. https://elasticsearch/logs/docker
    """
    addr = listen_addr, listen_port
    listen = loop.create_datagram_endpoint(GelfServerProtocol, local_addr=addr)
    transport, protocol = loop.run_until_complete(listen)
    protocol.verbose = verbose
    conn = aiohttp.TCPConnector(limit=es_conn_limit)
    protocol.es_session = aiohttp.ClientSession(loop=loop, connector=conn)
    protocol.es_url = elasticsearch_url
    protocol.instance_id = instance_id
    protocol.instance_ip = instance_ip
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    transport.close()
    loop.close()

gelf_es_relay()
