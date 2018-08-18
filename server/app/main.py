# -*- coding: utf-8 -*-


import falcon
import logging
logging.basicConfig(level=logging.INFO)

from app import controllers
from app import executors


logger = logging.getLogger('speedy_gonzales_server')


class RESTfulApplication(object):

    def __init__(self, logger, application, routes):
        self.logger = logger
        self.application = application
        self.routes = routes

    def expose(self):
        for (endpoint, controller) in self.routes.items():
            self.application.add_route(endpoint, controller)
            self.logger.info("Binding controller {} to route '{}'".format(controller, endpoint))
        self.logger.info('All routes exposed')


application = falcon.API()

spark_executor = executors.SparkExecutor()

routes = {
    '/': controllers.IndexController(),
    '/spark/{action}': controllers.SparkController(spark_executor),
    '/jobs/submit': controllers.SubmitJobController(spark_executor),
    '/jobs/{job_id}': controllers.SubmitJobController(spark_executor)
}


restful_application = RESTfulApplication(logger, application, routes)
restful_application.expose()
