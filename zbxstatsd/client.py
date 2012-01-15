# statsd.py

# Steve Ivy <steveivy@gmail.com>
# http://monkinetic.com

import logging
import socket
import random

# Sends statistics to the stats daemon over UDP
class Client(object):

    def __init__(self, host='127.0.0.1', port=8126, zabbix_hostname=None):
        """
        Create a new Statsd client.
        * host: the host where statsd is listening, defaults to 127.0.0.1
        * port: the port where statsd is listening, defaults to 8126

        >>> from pystatsd import statsd
        >>> stats_client = statsd.Statsd(host, port)
        """
        self.host = host
        self.port = int(port)
        self.zabbix_hostname = zabbix_hostname or socket.gethostname()
        self.log = logging.getLogger("zbxstatsd.client")
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def timing(self, stat, time, sample_rate=1):
        """
        Log timing information for a single stat
        >>> statsd_client.timing('some.time',0.5)
        """
        stats = {stat: "%d|ms" % (time*1000.0)}
        self.send(stats, sample_rate)

    def increment(self, stats, sample_rate=1):
        """
        Increments one or more stats counters
        >>> statsd_client.increment('some.int')
        >>> statsd_client.increment('some.int',0.5)
        """
        self.update_stats(stats, 1, sample_rate)

    def decrement(self, stats, sample_rate=1):
        """
        Decrements one or more stats counters
        >>> statsd_client.decrement('some.int')
        """
        self.update_stats(stats, -1, sample_rate)

    def update_stats(self, stats, delta=1, sampleRate=1):
        """
        Updates one or more stats counters by arbitrary amounts
        >>> statsd_client.update_stats('some.int',10)
        """
        if not isinstance(stats, list):
            stats = [stats]

        data = dict((stat, "%s|c" % delta) for stat in stats)
        self.send(data, sampleRate)

    def send(self, data, sample_rate=1):
        """
        Squirt the metrics over UDP
        """
        addr = (self.host, self.port)

        sampled_data = {}

        if sample_rate < 1:
            if random.random() > sample_rate:
                return
            sampled_data = dict((stat, "%s|@%s" % (value, sample_rate)) for stat, value in data.iteritems())
        else:
            sampled_data=data

        try:
            [self.udp_sock.sendto("%s:%s:%s" % (self.zabbix_hostname, stat, value), addr) for stat, value in sampled_data.iteritems()]
        except:
            self.log.exception("unexpected error")