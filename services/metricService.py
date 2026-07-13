#!/usr/bin/env python3
# Copyright 2023-2024 David Kneipp <david@davidkneipp.com>
# SPDX-License-Identifier: AGPL-3.0-or-later
import asyncio
import sys, os, json
import time, json
import socket
from prometheus_client import make_wsgi_app, start_http_server, Counter, Gauge, Summary, Histogram, CollectorRegistry
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from flask import Flask
from influxdb import InfluxDBClient
import threading
import traceback

sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/../lib"))

from messaging import RedisMessaging
from banners import Banners
from logtool import LogTool
from pyhss_config import config


class MetricService:

    def __init__(self, redisHost: str=None, redisPort: int=None):
        # PATCHED (phones-ansible, 2026-07-08): unlike every other pyHSS
        # service (e.g. hssService.py), this constructor never read the
        # 'redis' section of config.yaml/.env, so it always connected to
        # the hardcoded default of 127.0.0.1:6379 instead of the actual
        # redis host (e.g. "redis" in this Docker Compose stack). This was
        # the true root cause of every awaitMessage() failure: the metric
        # thread could never reach the real redis instance at all.
        self.redisUseUnixSocket = config.get('redis', {}).get('useUnixSocket', False)
        self.redisUnixSocketPath = config.get('redis', {}).get('unixSocketPath', '/var/run/redis/redis-server.sock')
        self.redisHost = redisHost if redisHost is not None else config.get('redis', {}).get('host', '127.0.0.1')
        self.redisPort = redisPort if redisPort is not None else config.get('redis', {}).get('port', 6379)
        self.redisMessaging = RedisMessaging(host=self.redisHost, port=self.redisPort, useUnixSocket=self.redisUseUnixSocket, unixSocketPath=self.redisUnixSocketPath)
        self.banners = Banners()
        self.logTool = LogTool(config=config)
        self.registry = CollectorRegistry(auto_describe=True)
        self.logTool.log(service='Metric', level='info', message=f"{self.banners.metricService()}", redisClient=self.redisMessaging)
        self.hostname = socket.gethostname()
        self.influxEnabled = config.get('influxdb', {}).get('enabled', None)
        self.influxDatabase = config.get('influxdb', {}).get('database', None)
        self.influxUser = config.get('influxdb', {}).get('username', None)
        self.influxPassword = config.get('influxdb', {}).get('password', None)
        self.influxHost = config.get('influxdb', {}).get('host', None)
        self.influxPort = config.get('influxdb', {}).get('port', None)

    def processInfluxdb(self, influxData: dict) -> bool:
        """
        Sends defined InfluxDB Metrics to InfluxDB, if configured.
        """

        if not self.influxEnabled:
            return True
        if not self.influxDatabase:
            return True
        if not self.influxUser:
            return True
        if not self.influxPassword:
            return True
        if not self.influxHost:
            return True
        if not self.influxPort:
            return True

        influxClient = InfluxDBClient(self.influxHost, self.influxPort, self.influxUser, self.influxPassword, self.influxDatabase)

        if not isinstance(influxData, list):
            influxData = [influxData]

        influxClient.write_points(influxData)

        return True


    def _processMetricMessage(self, metric):
        """
        Parses a single raw metric message (JSON list of metric dicts) and
        records it against the prometheus registry.
        """
        actions = {'inc': 'inc', 'dec': 'dec', 'set':'set'}
        prometheusTypes = {'counter': Counter, 'gauge': Gauge, 'histogram': Histogram, 'summary': Summary}

        self.logTool.log(service='Metric', level='debug', message=f"[Metric] [handleMetrics] Received Metric: {metric}", redisClient=self.redisMessaging)
        prometheusJsonList = json.loads(metric)

        for prometheusJson in prometheusJsonList:
            self.logTool.log(service='Metric', level='debug', message=f"[Metric] [handleMetrics] {prometheusJson}", redisClient=self.redisMessaging)
            if not all(key in prometheusJson for key in ('NAME', 'TYPE', 'ACTION', 'VALUE')):
                raise ValueError('All fields are not available for parsing')
            counterName = prometheusJson['NAME']
            counterType = prometheusTypes.get(prometheusJson['TYPE'].lower())
            counterAction = prometheusJson['ACTION'].lower()
            counterValue = float(prometheusJson['VALUE'])
            counterHelp = prometheusJson.get('HELP', '')
            counterLabels = prometheusJson.get('LABELS', {})

            try:
                metricInflux = prometheusJson.get('INFLUX', {})
                if metricInflux:
                    self.processInfluxdb(influxData=metricInflux)
            except Exception as e:
                self.logTool.log(service='Metric', level='warn', message=f"[Metric] [handleMetrics] Error processing metric InfluxDb content: {traceback.format_exc()}", redisClient=self.redisMessaging)

            if isinstance(counterLabels, list):
                        counterLabels = dict()

            if counterType is not None:
                try:
                    counterRecord = counterType(counterName, counterHelp, labelnames=counterLabels.keys(), registry=self.registry)
                    if counterLabels:
                        counterRecord = counterRecord.labels(*counterLabels.values())
                except ValueError as e:
                    counterRecord = self.registry._names_to_collectors.get(counterName)
                    if counterLabels and counterRecord:
                        counterRecord = counterRecord.labels(*counterLabels.values())
                action = actions.get(counterAction)
                if action is not None:
                    prometheusMethod = getattr(counterRecord, action)
                    prometheusMethod(counterValue)
                else:
                    self.logTool.log(service='Metric', level='warn', message=f"[Metric] [handleMetrics] Invalid action '{counterAction}' in message: {metric}, skipping.", redisClient=self.redisMessaging)
                    continue
            else:
                self.logTool.log(service='Metric', level='warn', message=f"[Metric] [handleMetrics] Invalid type '{counterType}' in message: {metric}, skipping.", redisClient=self.redisMessaging)
                continue

    def handleMetrics(self):
        """
        Collects queued metrics from redis, and exposes them using prometheus_client.

        PATCHED (phones-ansible, 2026-07-08): the original implementation
        called `awaitMessage(key='metric', usePrefix=True,
        prefixHostname=self.hostname, ...)`, blocking on a SINGLE queue key
        derived from this container's own `socket.gethostname()`. But
        producers (e.g. hssService.py) set their own `self.hostname` to
        their configured logical identity (e.g. `OriginHost`, like
        "hss.localdomain"), not the container hostname - so the consumer's
        key never matched any producer's key and no metric was ever
        collected, even when the connection was healthy. Fix: discover all
        `*:metric:metric` queues actually in use (regardless of which
        hostname/service produced them) and drain each one non-blockingly,
        instead of blocking on one hardcoded/mismatched key.
        """
        try:
            queues = self.redisMessaging.getQueues(pattern='*:metric:metric')
        except Exception:
            traceback.print_exc()
            return

        drainedAny = False
        for queue in queues:
            while True:
                metric = None
                try:
                    metric = self.redisMessaging.getMessage(queue=queue)
                    if not metric:
                        break
                    drainedAny = True
                    self._processMetricMessage(metric)
                except Exception as e:
                    self.logTool.log(service='Metric', level='error', message=f"[Metric] [handleMetrics] Unable to parse message: {metric}, due to {e}. Skipping.", redisClient=self.redisMessaging)
                    continue

        if not drainedAny:
            time.sleep(1)


    def getMetrics(self):
        while True:
            try:
                self.handleMetrics()
            except Exception:
                # PATCHED (phones-ansible, 2026-07-08): defense-in-depth -
                # never let an unexpected exception kill this thread
                # permanently; log and keep collecting.
                traceback.print_exc()
                time.sleep(1)


def main():
    metricService = MetricService()
    metricServiceThread = threading.Thread(target=metricService.getMetrics)
    metricServiceThread.start()

    prometheusWebClient = Flask(__name__)
    prometheusWebClient.wsgi_app = DispatcherMiddleware(prometheusWebClient.wsgi_app, {
        '/metrics': make_wsgi_app(registry=metricService.registry)
    })

    prometheusWebClient.run(host='0.0.0.0', port=9191)


if __name__ == '__main__':
    main()
