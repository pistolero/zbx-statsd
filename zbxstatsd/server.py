import re
from socket import AF_INET, SOCK_DGRAM, socket
import threading
import time
import types
import logging
import simplejson
import struct

try:
    from setproctitle import setproctitle
except ImportError:
    setproctitle = None

from daemon import Daemon


__all__ = ['Server']

def _clean_key(k):
    return re.sub(
        '[^a-zA-Z_\-0-9\.]',
        '',
        k.replace('/','-').replace(' ','_')
    )

def _recv_all(sock, count):
    buf = ''
    while len(buf)<count:
        chunk = sock.recv(count-len(buf))
        if not chunk:
            return buf
        buf += chunk
    return buf

class Server(object):

    def __init__(self, pct_threshold=90, debug=False, zabbix_host='localhost', zabbix_port=10051, flush_interval=10000):
        self.buf = 1024
        self.flush_interval = flush_interval
        self.pct_threshold = pct_threshold
        self.zabbix_host = zabbix_host
        self.zabbix_port = zabbix_port
        self.debug = debug

        self.counters = {}
        self.timers = {}
        self.flusher = 0


    def process(self, data):
        host, key, val = data.split(':')
        key = _clean_key(key)

        sample_rate = 1;
        fields = val.split('|')

        item_key = '%s:%s' % (host, key)
        
        if (fields[1] == 'ms'):
            if item_key not in self.timers:
                self.timers[item_key] = []
            self.timers[item_key].append(int(fields[0] or 0))
        else:
            if len(fields) == 3:
                sample_rate = float(re.match('^@([\d\.]+)', fields[2]).groups()[0])
            if item_key not in self.counters:
                self.counters[item_key] = 0;
            self.counters[item_key] += int(fields[0] or 1) * (1 / sample_rate)

    def flush(self):
        ts = int(time.time())
        stats = 0
        stat_string = ''
#        self.pct_threshold = 10
        
        data = []
        
        for k, v in self.counters.items():
            v = float(v) / (self.flush_interval / 1000)
            
            host, key = k.split(':',1)
            
            data.append({
                "host": host,
                "key": key,
                "value": str(v),
                "clock": ts
            })

            self.counters[k] = 0
            stats += 1

        for k, v in self.timers.items():
            if len(v) > 0:
                v.sort()
                count = len(v)
                min = v[0]
                max = v[-1]

                mean = min
                max_threshold = max
                median = min

                if count > 1:
                    thresh_index = int(round(count*float(self.pct_threshold)/100))#count - int(round((100.0 - self.pct_threshold) / 100) * count)
                    max_threshold = v[thresh_index - 1]
                    total = sum(v[:thresh_index])
                    mean = total / thresh_index
                    
                    if count%2 == 0:
                        median = (v[count/2] + v[count/2-1])/2.0
                    else:
                        median = (v[count/2])

                self.timers[k] = []

                host, key = _split_key(k)
                data.extend([{
                    "host": host,
                    "key": key + '[mean]',
                    "value": mean,
                    "clock": ts
                }, {
                    "host": host,
                    "key": key + '[upper]',
                    "value": max,
                    "clock": ts
                }, {
                    "host": host,
                    "key": key + '[lower]',
                    "value": min,
                    "clock": ts                    
                }, {
                    "host": host,
                    "key": key + '[count]',
                    "value": count,
                    "clock": ts
                }, {
                    "host": host,
                    "key": key + '[upper_%s]' % self.pct_threshold,
                    "value": max_threshold,
                    "clock": ts
                }, {
                    "host": host,
                    "key": key + '[median]',
                    "value": median,
                    "clock": ts
                }])

                stats += 1

#        data.append({
#                     
#        })
#        stat_string += 'statsd.numStats %s %d' % (stats, ts)
        self._send_metrics(data)

        self._set_timer()

        if self.debug:
            print data

    def _send_metrics(self, metrics):
        # Zabbix has very fragile Json parse, and we cannot use simplejson to dump whole packet
        j = simplejson.dumps
        metrics_data = []
        for m in metrics:
            metrics_data.append(('\t\t{\n'
                                 '\t\t\t"host":%s,\n'
                                 '\t\t\t"key":%s,\n'
                                 '\t\t\t"value":%s,\n'
                                 '\t\t\t"clock":%s}') % (j(m['host']), j(m['key']), j(m['value']), j(m['clock'])))
        json = ('{\n'
               '\t"request":"sender data",\n'
               '\t"data":[\n%s]\n'
               '}') % (',\n'.join(metrics_data))
        
        data_len = struct.pack('<Q', len(json))
        packet = 'ZBXD\1' + data_len + json        
        try:
            zabbix = socket()
            zabbix.connect((self.zabbix_host, self.zabbix_port))
            zabbix.sendall(packet)
            resp_hdr = _recv_all(zabbix, 13)
            if not resp_hdr.startswith('ZBXD\1') or len(resp_hdr) != 13:
                logging.error('Wrong zabbix response')
                return
            resp_body_len = struct.unpack('<Q', resp_hdr[5:])[0]
            resp_body = zabbix.recv(resp_body_len)
            resp = simplejson.loads(resp_body)
            logging.debug('Got response from Zabbix: %s' % resp)
            logging.info(resp.get('info'))
            if resp.get('response') != 'success':
                logging.error('Got error from Zabbix: %s', resp)
            zabbix.close()
        except:
            logging.exception('Error while sending data to Zabbix')
        
    def _set_timer(self):
        self._timer = threading.Timer(self.flush_interval/1000, self.flush)
        self._timer.start()

    def serve(self, hostname='', port=8126, zabbix_host='localhost', zabbix_port=2003):
        assert type(port) is types.IntType, 'port is not an integer: %s' % (port)
        addr = (hostname, port)
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.bind(addr)
        self.zabbix_host = zabbix_host
        self.zabbix_port = zabbix_port

        import signal
        import sys
        def signal_handler(signal, frame):
            self.stop()
        signal.signal(signal.SIGINT, signal_handler)

        self._set_timer()
        while 1:
            data, addr = self._sock.recvfrom(self.buf)
            self.process(data)

    def stop(self):
        self._timer.cancel()
        self._sock.close()


class ServerDaemon(Daemon):
    def run(self, options):
        if setproctitle:
            setproctitle('zbx-statsd')
        server = Server(pct_threshold=options.pct, debug=options.debug, flush_interval=options.flush_interval)
        server.serve(options.name, options.port, options.zabbix_host,
                     options.zabbix_port)


def main():
    import sys
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='debug mode', default=False)
    parser.add_argument('-n', '--name', dest='name', help='hostname to run on', default='')
    parser.add_argument('-p', '--port', dest='port', help='port to run on', type=int, default=8126)
    parser.add_argument('--zabbix-port', dest='zabbix_port', help='port to connect to zabbix on', type=int, default=10051)
    parser.add_argument('--zabbix-host', dest='zabbix_host', help='host to connect to zabbix on', type=str, default='localhost')
    parser.add_argument('-f', '--flush-interval', dest='flush_interval', help='interval between flushes', type=int, default=10000)
    parser.add_argument('-t', '--pct', dest='pct', help='stats pct threshold', type=int, default=90)
    parser.add_argument('-D', '--daemon', dest='daemonize', action='store_true', help='daemonize', default=False)
    parser.add_argument('--pidfile', dest='pidfile', action='store', help='pid file', default='/tmp/pystatsd.pid')
    parser.add_argument('--restart', dest='restart', action='store_true', help='restart a running daemon', default=False)
    parser.add_argument('--stop', dest='stop', action='store_true', help='stop a running daemon', default=False)
    options = parser.parse_args(sys.argv[1:])

    daemon = ServerDaemon(options.pidfile)
    if options.daemonize:
        daemon.start(options)
    elif options.restart:
        daemon.restart(options)
    elif options.stop:
        daemon.stop()
    else:
        daemon.run(options)
        
if __name__ == '__main__':
    main()