import logging
log = logging.getLogger('zen.HubService.SEFService')

import Globals
from Products.ZenCollector.services.config import CollectorConfigService
from Products.ZenHub.zodb import onUpdate, onDelete

class FakeDevice(object):
    id = 'SEF dummy payload'

class SEFService(CollectorConfigService):
    def _filterDevices(self, deviceList):
        return [ FakeDevice() ]

    def _createDeviceProxy(self, device):
        proxy = CollectorConfigService._createDeviceProxy(self, device)
        proxy.configCycleInterval = 3600
        proxy.name = "SEF Configuration"
        proxy.device = device.id

        return proxy


if __name__ == '__main__':
    from Products.ZenHub.ServiceTester import ServiceTester
    tester = ServiceTester(SEFService)
    def printer(config):
        print "Plop!"
    tester.printDeviceProxy = printer
    tester.showDeviceInfo()

