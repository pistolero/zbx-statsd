import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "zbx-statsd",
    version = "0.4.0",
    author = "Sergey Kirillov",
    author_email = "sergey.kirillov@gmail.com",
    description = ("Clone of Etsy's statsd and Steve Ivy's py-statsd designed to work with Zabbix (http://www.zabbix.com/)."),
    url='https://github.com/pistolero/zbx-statsd',
    license = "BSD",
    packages=['zbxstatsd'],
    long_description=read('README.txt'),
    install_requires=['argparse', 'zbxsend'],
    classifiers=[
        "License :: OSI Approved :: BSD License",
    ],
    entry_points={
        'console_scripts': [
              'zbx-statsd = zbxstatsd.server:main',
        ]
    }
)