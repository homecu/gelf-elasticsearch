#!/usr/bin/env python3

import aiohttp
import asyncio
import datetime
import gzip
import itertools
import json
import logging.handlers
import random
import re
import shellish.logging

root_logger = logging.getLogger()
root_logger.addHandler(shellish.logging.VTMLHandler())
logger = logging.getLogger('relay')

_prio_names = logging.handlers.SysLogHandler.priority_names
log_level_map = dict(map(reversed, _prio_names.items()))
loop = asyncio.get_event_loop()


class GelfServerProtocol(object):

    image_re = re.compile('((?P<repo>.*?)/)?(?P<tag>[^:]*)(:(?P<version>.*))?')
    attempts = 5
    identer = itertools.count()

    def connection_made(self, transport):
        pass

    def datagram_received(self, data, addr):
        log = json.loads(gzip.decompress(data).decode())
        ts = datetime.datetime.utcfromtimestamp(log['timestamp'])
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
            "timestamp": ts.isoformat(),
        }
        loop.create_task(self.relaylog(record, ts))

    async def relaylog(self, log, ts):
        ident = next(self.identer)
        data = json.dumps(log)
        url = '%s/%s-%s/%s' % (self.es_url, self.es_index,
                               ts.strftime('%Y-%m-%d'), self.es_type)
        for retry in range(self.attempts):
            try:
                await self._relaylog(data, url, ident)
            except asyncio.TimeoutError:
                logger.warning('ES Timeout [%d]' % ident)
            except (IOError, aiohttp.ClientResponseError) as e:
                logger.error('ES %s [%d]: %s' % (type(e).__name__, ident, e))
            except Exception:
                logger.exception('ES Exception [%d]' % ident)
            else:
                return
            # Only Failures...
            await self.close_es_session()
            backoff = (random.random() * 60) * (retry + 1)
            logger.warning("Will retry message %d in %g seconds." % (ident,
                           backoff))
            await asyncio.sleep(backoff)
        logger.error("Dropped message %d after %d attempts: %s" % (ident,
                     self.attempts, data))

    async def _relaylog(self, data, url, ident):
        es = self.get_es_session()
        with aiohttp.Timeout(60):
            async with es.post(url, data=data) as r:
                if r.status != 201:
                    raise IOError(await r.text())
                else:
                    if self.verbose:
                        logger.info('SUCCESS [%d]: %s' % (ident,
                                    await r.text()))

    def get_es_session(self):
        try:
            return self._es_session
        except AttributeError:
            logging.info("Starting new ES session: %s" % self.es_url)
            conn = aiohttp.TCPConnector(limit=self.es_conn_limit)
            self._es_session = s = aiohttp.ClientSession(loop=loop,
                                                         connector=conn)
            return s

    async def close_es_session(self):
        try:
            session = self._es_session
        except AttributeError:
            return
        del self._es_session
        logging.warning("Closing ES session")
        await session.close()


@shellish.autocommand
def gelf_es_relay(elasticsearch_url, es_index='logging', es_type='docker',
                  listen_addr='0.0.0.0', listen_port=12201, verbose=False,
                  es_conn_limit=100, instance_id=None, instance_ip=None,
                  log_level='info'):
    """ A Gelf server relay to elasticsearch.

    The URL should just be the scheme://host:port/ without any index or type.
    The index will be modified to include a UTC date suffix. """
    root_logger.setLevel(log_level.upper())
    addr = listen_addr, listen_port
    listen = loop.create_datagram_endpoint(GelfServerProtocol, local_addr=addr)
    transport, protocol = loop.run_until_complete(listen)
    protocol.verbose = verbose
    protocol.es_url = elasticsearch_url.rstrip('/')
    protocol.es_index = es_index
    protocol.es_type = es_type
    protocol.es_conn_limit = es_conn_limit
    protocol.instance_id = instance_id
    protocol.instance_ip = instance_ip
    logger.info("Starting GELF ES Relay: %s" % elasticsearch_url)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    transport.close()
    loop.close()

gelf_es_relay()
