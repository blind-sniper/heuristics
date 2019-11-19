#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.mongodb import MongodbConnector
from catenae.connectors.aerospike import AerospikeConnector
from catenae import Link, Electron
import logging


class StatsDumper(Link):
    def setup(self):
        self.aerospike.set_defaults('fuc_benchmark')
        self.mongodb.set_defaults('fuc_benchmark', 'stats')
        # Users ranking indexes
        self.mongodb.create_index('timestamp')
        self.mongodb.create_index('event')

    def transform(self, electron):
        event = electron.value['event']

        # +1 USER CLASSIFIED EVENT
        # iNCREMENTAR POSITIVOS CON UMBRAL CONFIGURABLE
        # INCREMENTAR NUEVOS

        if event == 'user_classifier_user_prediction':
            self.logger.log(f'PREDICTION - {electron.value}')
            self.mongodb.put(electron.value)

        # Do not store updated_user events
        elif event == 'user_updater_updated_user':
            pass

        elif event == 'user_explorer_seen_user':
            user_id = electron.value['value']['user_id']
            if not self.aerospike.exists(user_id, set_='seen_users'):
                self.aerospike.put(user_id, set_='seen_users')
                self.mongodb.put(electron.value)

        elif event == 'user_explorer_seen_subreddit':
            subreddit_id = electron.value['value']['subreddit_id']
            if not self.aerospike.exists(subreddit_id, set_='seen_subreddits'):
                self.aerospike.put(subreddit_id, set_='seen_subreddits')
                self.mongodb.put(electron.value)

        # Store other events
        else:
            self.mongodb.put(electron.value)

        self.logger.log(electron.value, level='debug')


if __name__ == "__main__":
    StatsDumper(log_level='info').start()
