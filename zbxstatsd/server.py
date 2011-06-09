import re
from socket import AF_INET, SOCK_DGRAM, socket
import threading
import time
import types
import logging

from zbxsend import Metric, send_to_zabbix 

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
        try:
            host, key, val = data.split(':')
        except ValueError:
            logging.info('Got invalid data packet. Skipping')
            logging.debug('Data packet dump: %r' % data)
            return
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
        
        metrics = []
        
        for k, v in self.counters.items():
            v = float(v) / (self.flush_interval / 1000)
            
            host, key = k.split(':',1)
            
            metrics.append(Metric(host, key, str(v), ts))

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

                host, key = k.split(':', 1)
                metrics.extend([
                    Metric(host, key + '[mean]', mean, ts),
                    Metric(host, key + '[upper]', max, ts),
                    Metric(host, key + '[lower]', min, ts),
                    Metric(host, key + '[count]', count, ts),
                    Metric(host, key + '[upper_%s]' % self.pct_threshold, max_threshold, ts),
                    Metric(host, key + '[median]', median, ts),
                ])

                stats += 1

#        stat_string += 'statsd.numStats %s %d' % (stats, ts)

        send_to_zabbix(metrics, self.zabbix_host, self.zabbix_port)

        self._set_timer()

        if self.debug:
            print metrics

        
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
            setproctitle('zbxstatsd')
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
    parser.add_argument('-l', dest='log_file', help='log file', type=str, default=None)
    parser.add_argument('-f', '--flush-interval', dest='flush_interval', help='interval between flushes', type=int, default=10000)
    parser.add_argument('-t', '--pct', dest='pct', help='stats pct threshold', type=int, default=90)
    parser.add_argument('-D', '--daemon', dest='daemonize', action='store_true', help='daemonize', default=False)
    parser.add_argument('--pidfile', dest='pidfile', action='store', help='pid file', default='/tmp/pystatsd.pid')
    parser.add_argument('--restart', dest='restart', action='store_true', help='restart a running daemon', default=False)
    parser.add_argument('--stop', dest='stop', action='store_true', help='stop a running daemon', default=False)
    options = parser.parse_args(sys.argv[1:])

    logging.basicConfig(level=logging.DEBUG if options.debug else logging.WARN,
                        stream=open(options.log_file) if options.log_file else sys.stderr)

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