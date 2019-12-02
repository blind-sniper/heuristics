#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.mongodb import MongodbConnector
from catenae.connectors.aerospike import AerospikeConnector
from catenae import Link, Electron, utils, CircularOrderedSet, ThreadingQueue, rpc
import crawler_helper as rch
import logging
import random
import time
import threading


class SubredditExplorer(Link):
    def setup(self):
        self.subreddits_queue = ThreadingQueue(size=1, circular=True)
        self.seen_subreddits = CircularOrderedSet(50)
        explorer = threading.Thread(target=self.explorer)
        explorer.start()
        self.spider_name = rch.get_spider_name('FUC')

        self.mongodb.set_defaults('fuc_benchmark', 'subreddits')
        self.aerospike.set_defaults('fuc_benchmark')
        self.loop(self.suicide, interval=3300, wait=True)

    def send_seen_user_event(self, user_id):
        electron = Electron(value={
            'event': 'user_explorer_seen_user',
            'timestamp': utils.get_timestamp_ms(),
            'value': {
                'user_id': user_id
            }
        },
                            topic='stats')
        self.logger.log(electron.value)
        self.send(electron)

    def send_seen_subreddit_event(self, subreddit_id):
        electron = Electron(value={
            'event': 'user_explorer_seen_subreddit',
            'timestamp': utils.get_timestamp_ms(),
            'value': {
                'subreddit_id': subreddit_id
            }
        },
                            topic='stats')
        self.logger.log(electron.value)
        self.send(electron)

    def _process_new_submissions(self, subreddit='r/all'):
        for submission in rch.get_subreddit_submissions_elements(subreddit, self.spider_name, 100):
            try:
                # Filter repeated submissions
                submission_id = rch.get_submission_id(submission)

                if not self.aerospike.exists(submission_id, set_name='seen_submissions'):
                    self.aerospike.put(submission_id, set_name='seen_submissions')
                else:
                    continue

                # Initializer mode, it only makes sense for r/all
                if subreddit == 'r/all':
                    # The text may be posted to its own profile
                    subreddit_id = rch.get_subreddit_id(submission)
                    if subreddit_id:
                        self.send(Electron(value=subreddit_id, topic='subreddits'))
                        self.send_seen_subreddit_event(subreddit_id)

                user_id = rch.get_user_id(submission)
                if not user_id:
                    continue
                self.logger.log(f'  + [text] {user_id} ({submission_id})', level='debug')

                text = rch.get_submission_title(submission)

                # Number of comments
                comments = int(rch.get_submission_no_comments(submission))

                # Points
                points = int(rch.get_submission_score(submission))

                # Timestamp
                timestamp = int(rch.get_submission_timestamp(submission))

            except Exception:
                self.logger.log(level='exception')

            # Messages keyed by user_id
            electron = Electron(user_id, {
                'comments': comments,
                'points': points,
                'timestamp': timestamp,
                'text': text
            })
            self.send_seen_user_event(user_id)
            self.send(electron)
            self.logger.log(f'[SUBMISSION] [{electron.key}] {electron.value}', level='debug')

    def _process_new_comments(self, subreddit='r/all'):
        for comment in rch.get_subreddit_comments_elements(subreddit, self.spider_name, 100):
            try:
                # Filter repeated comments
                comment_id = rch.get_comment_id(comment)

                if not self.aerospike.exists(comment_id, set_name='seen_comments'):
                    self.aerospike.put(comment_id, set_name='seen_comments')
                else:
                    continue

                # Initializer mode, it only makes sense for r/all
                if subreddit == 'r/all':
                    # The text may be posted to its own profile
                    subreddit_id = rch.get_comment_subreddit_id(comment)
                    if subreddit_id:
                        self.send(Electron(value=subreddit_id, topic='subreddits'))
                        self.send_seen_subreddit_event(subreddit_id)
                else:
                    subreddit_id = subreddit

                user_id = rch.get_comment_user_id(comment)
                if not user_id:
                    continue
                self.logger.log(f'  + [text] {user_id} ({comment_id})', level='debug')

                text = rch.get_comment_body(comment)

                # Number of comments
                comments = 0

                # Points
                points = 1

                # Timestamp
                timestamp = int(rch.get_comment_timestamp(comment))

            except Exception:
                self.logger.log(level='exception')

            # Messages keyed by user_id
            electron = Electron(user_id, {
                'comments': comments,
                'points': points,
                'timestamp': timestamp,
                'text': text
            })
            self.send_seen_user_event(user_id)
            self.send(electron)
            self.logger.log(f'[COMMENT] [{electron.key}] {electron.value}', level='debug')

    def reject_request(self, context):
        self.logger.log('Subreddit request rejected', level='warn')
        time.sleep(5)
        self.rpc_notify('request_subreddit', to='SubredditChooser')

    @rpc
    def put_subreddit(self, context, subreddit_id):
        if subreddit_id not in self.seen_subreddits:
            self.logger.log(f'Received VALID subreddit {subreddit_id} via RPC')
            self.subreddits_queue.put(subreddit_id)
            return
        self.logger.log(f'Received NOT VALID subreddit {subreddit_id} via RPC')
        self.rpc_notify('request_subreddit', to='SubredditChooser')

    def _explore(self, subreddit_id):
        self.logger.log(f'Exploring subreddit: {subreddit_id}')
        self._process_new_submissions(subreddit_id)
        self._process_new_comments(subreddit_id)

    def explorer(self):
        running = True
        while (running):
            self.logger.log(f'Subreddit requested')
            self.rpc_notify('request_subreddit', to='SubredditChooser')
            try:
                subreddit_id = self.subreddits_queue.get()
                if subreddit_id not in self.seen_subreddits:
                    self.seen_subreddits.add(subreddit_id)
                    self._explore(subreddit_id)
                else:
                    self.logger.log(f'{subreddit_id} is known, skipping', level='warn')
            except Exception:
                self.logger.log(level='exception')


if __name__ == "__main__":
    SubredditExplorer(log_level='info', synchronous=True).start()
