# -*- coding: utf-8 -*-


import falcon

from app import actions
from app import executors


class RESTfulApplication(object):

    def __init__(self, application, routes):
        self.application = application
        self.routes = routes

    def expose(self):
        for (endpoint, action) in self.routes.items():
            self.application.add_route(endpoint, action)


application = falcon.API()

spark_executor = executors.SparkExecutor()

routes = {
    '/': actions.IndexAction(),
    '/spark': actions.SparkAction(spark_executor),
    '/jobs/submit': actions.SubmitJobAction(spark_executor)
}


restful_application = RESTfulApplication(application, routes)
restful_application.expose()
