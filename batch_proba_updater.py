#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.mongodb import MongodbConnector
from catenae import Link, Electron, utils
from feature_aggregator import FeatureAggregator
import crawler_helper as rch
import logging
from catenae import utils
from depression_classifier import DepressionClassifier


class BatchProbaUpdater(Link):
    def setup(self):
        if len(self.args) < 1:
            self.suicide('the threshold parameter was not provided.')

        self.threshold = float(self.args[0])
        if self.threshold == 0:
            self.suicide('pseudofeedback training is disabled.')

        self.mongodb.set_defaults('fuc_benchmark', 'users_ranking')
        self.classifier = DepressionClassifier()

    # RPC method invoked from a model_trainer instance
    def update_model(self, context, paths):
        self.logger.log('Updating model...', level='debug')
        self.classifier.update_model(paths)
        self.logger.log('Model updated')
        self.batch_proba_update()

    def batch_proba_update(self):
        result = self.mongodb.get(sort=[('proba.value', -1)], limit=50000)
        for user in result:
            self.logger.log(f"Updating probability for user {user['user_id']}")
            new_proba = self.classifier.predict(user['proba']['texts'])
            self.mongodb.update(query={'user_id': user['user_id']}, value={'proba.value': new_proba})


if __name__ == "__main__":
    BatchProbaUpdater(log_level='info').start()