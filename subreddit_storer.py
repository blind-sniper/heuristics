#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.aerospike import AerospikeConnector
from catenae.connectors.mongodb import MongodbConnector
from catenae import Link, Electron
import logging


class SubredditStorer(Link):
    def setup(self):
        self.aerospike.set_defaults('fuc_benchmark', 'subreddits')
        self.mongodb.set_defaults('fuc_benchmark', 'subreddits')
        self.mongodb.create_index('subreddit_id')
        self.mongodb.create_index('index')

        self.next_index = \
            self.mongodb.client.fuc_benchmark.subreddits.count_documents(filter={})

    def transform(self, electron):
        subreddit_id = electron.value
        self.logger.log(subreddit_id)

        if self.aerospike.exists(subreddit_id):
            return

        self.aerospike.put(subreddit_id)
        self.mongodb.put({'subreddit_id': subreddit_id, 'index': self.next_index})
        self.next_index += 1


if __name__ == "__main__":
    SubredditStorer(log_level='info').start()
