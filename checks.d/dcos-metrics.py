from urlparse import urljoin
import requests
from datadog_checks.checks import AgentCheck
from datadog_checks.config import _is_affirmative
import json

class DcosMetrics(AgentCheck):
    METRICS_API_ROUTE = "http://localhost:61001/system/v1/metrics/v0"
    NODE_METRICS_URL = METRICS_API_ROUTE + "/node"
    GET_CONTAINERS_URL = METRICS_API_ROUTE + "/containers"
    METRICS_PREFIX = ""
    DEFAULT_TAGS = ['source:dcos-metrics']

    def check(self, instance):
        try:
            instance_tags, skip_node_metrics, metrics_prefix = self.get_instance_config(instance)
        except Exception as e:
            self.log.error("Invalid instance configuration.")
            raise e
        if skip_node_metrics == False:
            self.push_dcos_metrics(self.NODE_METRICS_URL, instance_tags)
        self.METRICS_PREFIX = metrics_prefix
        self.process_containers_metrics(instance_tags)

    def get_json(self, url):
        try:
            r = requests.get(url)
            if r.status_code == 204:
                print ("No reposnse data for url %s as response code = 204" % (url))
                return None
            if r.status_code != 200:
                print ("Non Success Response code for url  %s, with response code %s and message %s" % (url,r.status_code, r.text))
                return None
        except Exception as e:
            print ("Exception for url %s with message %s" % (url,e.message))
            return None
        return r.json()

    def process_containers_metrics(self,instace_tags=None):
        contJson = self.get_json(self.GET_CONTAINERS_URL)
        if contJson is not None:
            for x in contJson:
                contMetricsUrl = self.GET_CONTAINERS_URL + "/" + str(x)
                contAppMetricsUrl = contMetricsUrl + "/app"
                self.push_dcos_metrics(contMetricsUrl,instace_tags)
                self.push_dcos_metrics(contAppMetricsUrl,instace_tags)

    def push_dcos_metrics(self,metricsUrl,instance_tags=None):
        metricsJson = self.get_json(metricsUrl)
        if metricsJson is not None:
            self.process_datapoints(metricsJson,instance_tags)

    def process_datapoints(self,metricsJson, instance_tags=None):
        dimensions = ['']
        dpRoot = metricsJson
        if dpRoot['dimensions'] is not None:
            dimensions = dpRoot['dimensions']
        allTags = ['']
        for app in dpRoot['datapoints']:
            if 'tags' not in app:
                allTags=self.get_metric_tags(dimensions, instance_tags)
            else:
                allTags=self.get_metric_tags(app['tags'],self.get_metric_tags(dimensions, instance_tags))
            if self.METRICS_PREFIX != "":
                self.gauge(self.METRICS_PREFIX + "." + app['name'] , app['value'], tags=allTags)
            else:
                self.gauge(app['name'] , app['value'], tags=allTags)

    def get_metric_tags(self, dp, tags=None):
        dictList = []
        for key, value in dp.iteritems():
            temp = str(key)+":"+str(value)
            dictList.append(temp)
        dictList = dictList + tags + self.DEFAULT_TAGS
        return dictList

    def get_instance_config(self, instance):
        tags = instance.get('tags', [])
        skip_node_metrics = instance.get('skip_node_metrics', False)
        metrics_prefix = instance.get('metrics_prefix', self.METRICS_PREFIX)
        return tags, skip_node_metrics, metrics_prefix