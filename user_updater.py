#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.mongodb import MongodbConnector
from catenae import Link, Electron, utils
from feature_aggregator import FeatureAggregator
import crawler_helper as rch
import logging
import threading


class UserUpdater(Link):
    def setup(self):
        self.model_update_lock = threading.Lock()
        self.mongodb.set_defaults('fuc_benchmark', 'users_ranking')

        # Users ranking indexes
        self.mongodb.create_index('user_id', unique=True)
        self.mongodb.create_index('comments.avg')
        self.mongodb.create_index('points.avg')
        self.mongodb.create_index('hour.avg')
        self.mongodb.create_index('proba.value')
        self.mongodb.create_index('proba.timestamp')

        # Fusion hierarchy indexes
        self.mongodb.create_index(keys=[('proba.value', -1), ('hour.dis06', 1)])
        # self.mongodb.create_index(keys=[('proba.value', -1), ('comments.avg', -1)])

        self.aggregator = FeatureAggregator()

        # Suicide periodically to "fix" Pickle memory leaks
        self.loop(self.suicide, interval=900, wait=True)  # 15 min

    # RPC method invoked from a model_trainer instance
    def update_model(self, context, paths):
        self.logger.log('Updating model...', level='debug')
        self.model_update_lock.acquire()
        self.aggregator.classifier.update_model(paths)
        self.model_update_lock.release()
        self.logger.log('Model updated')

    def transform(self, electron):
        user_id = electron.key

        comments = electron.value['comments']
        points = electron.value['points']
        timestamp = electron.value['timestamp']
        text = electron.value['text']

        self.logger.log(f'Updating ranking for user {user_id}')
        self.logger.log(f'comments {comments}', level='debug')
        self.logger.log(f'points {points}', level='debug')
        self.logger.log(f'timestamp {timestamp}', level='debug')

        # Retrieve current tuple
        result = self.mongodb.get(query={'user_id': user_id}, limit=1)
        try:
            measures = next(result)
        except Exception:
            # New user
            measures = {'user_id': user_id}

        measures = {'user_id': user_id}

        # Avg. comments
        measures.update(self.aggregator.get_comments(measures, comments))

        # Avg. points
        measures.update(self.aggregator.get_points(measures, points))

        # Avg. hour, hour distances
        measures.update(self.aggregator.get_hour(measures, timestamp))

        # Aggregated proba
        self.model_update_lock.acquire()
        measures.update(self.aggregator.get_proba(measures, text))
        self.model_update_lock.release()

        self.mongodb.update({'user_id': user_id}, measures)
        self.logger.log(measures, level='debug')

        # Stats message
        electron = Electron(value={
            'event': 'user_updater_updated_user',
            'timestamp': utils.get_timestamp_ms(),
            'value': {
                'user_id': user_id
            }
        })
        return electron


if __name__ == "__main__":
    UserUpdater(log_level='info').start()
