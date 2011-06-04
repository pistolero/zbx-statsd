Introduction
------------

zbx-statsd is a clone of Etsy's statsd and Steve Ivy's py-statsd designed to work with Zabbix as stats collection and graphing backend. Based on sources of pystatsd.

* pystatsd
	- https://github.com/sivy/py-statsd/
* Graphite
    - http://graphite.wikidot.com
* Statsd 
    - code: https://github.com/etsy/statsd
    - blog post: http://codeascraft.etsy.com/2011/02/15/measure-anything-measure-everything/

Usage
-------------

Client:
    from zbx-statsd import Client, Server

    sc = Client('example.org',8125, 'zabbix_name_of_this_machine')

    sc.timing('python_test.time',500)
    sc.increment('python_test.inc_int')
    sc.decrement('python_test.decr_int')

Server:
    srvr = Server(debug=True)
    srvr.serve()
