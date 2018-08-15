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

routes = {
    '/': actions.IndexAction(),
    '/jobs/submit': actions.SubmitAction(executors.SparkExecutor())
}


restful_application = RESTfulApplication(application, routes)
restful_application.expose()
