#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.mongodb import MongodbConnector
from catenae import (Link, Electron, rpc)
import logging
import time
import random


class SubredditChooser(Link):
    """ Now, the subreddit is choosen at random and this module is not really
    needed. However, when implementing a subreddit ranking this module will be
    mandatory, analogously to the user chooser module.
    """

    def setup(self):
        self.initialized = False
        self.mongodb.set_defaults('fuc_benchmark', 'subreddits')
        self.loop(self.suicide, interval=3400, wait=True)  # 1 hour

    def _get_random_subreddit(self):
        # subreddit_doc = self.mongodb.get_random()
        #
        # # Initializer mode if a subreddit cannot be retrieved
        # if not subreddit_doc:
        #     self.logger.log(f'Exploring /r/all')
        #     self._process_new_submissions()
        #     self._process_new_comments()
        #     self.generator()
        #     return

        # FIXED RANDOM FOR BENCHMARK
        collection_size = \
            self.mongodb.client.fuc_benchmark.subreddits \
            .count_documents(filter={})
        if self.initialized and collection_size > 0:
            index = random.randint(0, collection_size - 1)
            subreddit_doc = next(self.mongodb.get({'index': index}))
            subreddit_id = subreddit_doc['subreddit_id']
        else:
            subreddit_id = 'r/all'
        if subreddit_id == 'r/all' and self.initialized:
            time.sleep(.5)
            return
        if subreddit_id == 'r/all':
            self.initialized = True
        return subreddit_id

    @rpc
    def request_subreddit(self, context, attempts=10):
        subreddit_explorer = context['uid']
        self.logger.log(f'Request received from explorer {subreddit_explorer}')

        try:
            subreddit_id = self._get_random_subreddit()
            if subreddit_id == None:
                raise ValueError
            self.logger.log(f'Selected subreddit: {subreddit_id} for explorer {subreddit_explorer}')
            self.rpc_call(subreddit_explorer, 'put_subreddit', subreddit_id)

        except Exception:
            self.logger.log(level='exception')
            self.logger.log(f'Cannot retrieve a subreddit for explorer {subreddit_explorer}',
                            level='warn')
            if attempts > 0:
                time.sleep(0.5)
                self.request_subreddit(context, attempts - 1)
            else:
                self.rpc_call(subreddit_explorer, 'reject_request')


if __name__ == "__main__":
    SubredditChooser(log_level='info', synchronous=True).start()
