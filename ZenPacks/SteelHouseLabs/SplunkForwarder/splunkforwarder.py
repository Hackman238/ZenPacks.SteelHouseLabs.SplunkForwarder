import time
import socket
import os
import logging

from twisted.internet import reactor, defer
from twisted.python import failure

import json
import urllib
import urllib2

import pika

import Globals
import zope.interface
import zope.component

from Products.ZenUtils.GlobalConfig import getGlobalConfiguration

from Products.ZenCollector.daemon import CollectorDaemon
from Products.ZenCollector.interfaces import ICollector, ICollectorPreferences,\
                                             IEventService, \
                                             IScheduledTask, IStatisticsService
from Products.ZenCollector.tasks import SimpleTaskFactory,\
                                        SimpleTaskSplitter,\
                                        BaseTask, TaskStates
from Products.ZenUtils.observable import ObservableMixin

from Products.ZenUtils.Utils import zenPath

from Products.ZenEvents.EventServer import Stats
from Products.ZenUtils.Utils import unused
from Products.ZenCollector.services.config import DeviceProxy

from ZenPacks.SteelHouseLabs.SplunkForwarder.services.SEFService import SEFService
unused(Globals, DeviceProxy)

COLLECTOR_NAME = 'splunkforwarder'
log = logging.getLogger("zen.%s" % COLLECTOR_NAME)

global_conf = getGlobalConfiguration()

splukloggerfile = global_conf.get('splunkLoggerFile', '/opt/zenoss/var/splunklogger')

exchange = 'events'
queue = 'eventForwarder'
passwd = global_conf.get('amqppassword', 'zenoss')
user = global_conf.get('amqpuser', 'zenoss')
vhost = global_conf.get('amqpvhost', '/zenoss')
port = int(global_conf.get('amqpport', '5672'))
host = global_conf.get('amqphost', 'localhost')


class SEFPrefs(object):
    zope.interface.implements(ICollectorPreferences)

    def __init__(self):
        """
        Constructs a new ColPrefs instance and
        provides default values for needed attributes.
        """
        self.collectorName = COLLECTOR_NAME
        self.defaultRRDCreateCommand = None
        self.configCycleInterval = 20 # minutes
        self.cycleInterval = 5 * 60 # seconds

        # The configurationService attribute is the fully qualified class-name
        # of our configuration service that runs within ZenHub
        self.configurationService = 'ZenPacks.SteelHouseLabs.SplunkForwarder.services.SEFService'

        # Will be filled in based on buildOptions
        self.options = None

        self.configCycleInterval = 20*60

    def postStartupTasks(self):
        task = SEFTask(COLLECTOR_NAME, configId=COLLECTOR_NAME)
        yield task

    def buildOptions(self, parser):
        """
        Command-line options to be supported
        """
        pass


    def postStartup(self):
        daemon = zope.component.getUtility(ICollector)
        
        # add our collector's custom statistics
        statService = zope.component.queryUtility(IStatisticsService)
        statService.addStatistic("events", "COUNTER")


class SEFTask(BaseTask):
    """
    Consume the eventForwarder queue for messages and turn them into events
    """
    zope.interface.implements(IScheduledTask)

    DATE_FORMAT = '%b %d %H:%M:%S'
    SAMPLE_DATE = 'Apr 10 15:19:22'

    log.info('Setting up Splunk loggerfile...')
    dataLog = open(splukloggerfile, "wb+")

    log.info('Setting up AMQP...')
    credentials = pika.PlainCredentials(user, passwd)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host = host,
                                             port = port,
                                             virtual_host = vhost,
                                             credentials = credentials))

    channel = connection.channel()

    channel.queue_bind(exchange='events',
                        queue=queue)


    def __init__(self, taskName, configId,
                 scheduleIntervalSeconds=3600, taskConfig=None):
        BaseTask.__init__(self, taskName, configId,
                 scheduleIntervalSeconds, taskConfig)

        self.log = log

        # Needed for ZCA interface
        self.name = taskName
        self.configId = configId
        self.state = TaskStates.STATE_IDLE
        self.interval = scheduleIntervalSeconds
        self._preferences = taskConfig
        self._daemon = zope.component.getUtility(ICollector)
        self._eventService = zope.component.queryUtility(IEventService)
        self._statService = zope.component.queryUtility(IStatisticsService)
        self._preferences = self._daemon

        self.options = self._daemon.options

        self._daemon.changeUser()
        self.processor = None

        log.info('Starting AMQP consumption...')
        self.channel.basic_consume(self.callback,
                              queue=queue,
                              no_ack=True)

        self.channel.start_consuming()


    def callback(self, ch, method, properties, body):
        self.log.debug(" [EVENT] %s" % (body,))
        self.passEvent(body)


    def passEvent(self, evtBody):
        evt = eval(evtBody)
        self.writeToLog(evt)


    def writeToLog(self, data):
        self.dataLog.write(str(data) + '\n')
        self.datalog.flush()


    def doTask(self):
        """
        This is a wait-around task since we really are called
        asynchronously.
        """
        return defer.succeed("Waiting for messages...")

    def cleanup(self):
        self.log.info('cleanup')


    @defer.inlineCallbacks
    def _shutdown(self, *ignored):
        self.log.info("Shutting down...")
        self.log.info("Closing log...")
        self.dataLog.close()
        self.log.info("Closing AMQP connection...")
        self.channel.close()
        self.connection.close()


class SEFConf(ObservableMixin):
    """
    Receive a configuration object
    """
    zope.interface.implements(IScheduledTask)

    def __init__(self, taskName, configId,
                 scheduleIntervalSeconds=3600, taskConfig=None):
        super(SEFConf, self).__init__()

        # Needed for ZCA interface
        self.name = taskName
        self.configId = configId
        self.state = TaskStates.STATE_IDLE
        self.interval = scheduleIntervalSeconds
        self._preferences = taskConfig
        self._daemon = zope.component.getUtility(ICollector)


    def doTask(self):
        return defer.succeed("Success")

    def cleanup(self, body):
        pass

class SEFDaemon(CollectorDaemon):
   pass

if __name__=='__main__':
    Preferences = SEFPrefs()
    TaskFactory = SimpleTaskFactory(SEFConf)
    TaskSplitter = SimpleTaskSplitter(TaskFactory)
    zef = SEFDaemon(Preferences, TaskSplitter)
    zef.run()
