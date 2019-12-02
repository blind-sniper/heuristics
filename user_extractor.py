#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, CircularOrderedSet, ThreadingQueue, rpc
import logging
import crawler_helper as rch
import time
import threading


class UserExtractor(Link):
    def setup(self):
        self.spider_name = rch.get_spider_name('FUC')
        self.users_ranking_queue = ThreadingQueue(size=1, circular=True)
        self.seen_users = CircularOrderedSet(50)
        extractor = threading.Thread(target=self.extractor)
        extractor.start()
        self.loop(self.suicide, interval=3500, wait=True)

    def _get_user_texts(self, user_id):
        for text in self._get_user_submissions(user_id):
            yield text
        for text in self._get_user_comments(user_id):
            yield text

    def _get_user_submissions(self, user_id):
        for element in rch.get_user_submissions_elements(self.spider_name, user_id,
                                                         items_no=100)[::-1]:
            try:
                subreddit_id = rch.get_subreddit_id(element)
                if subreddit_id:
                    self.send(Electron(value=subreddit_id, topic='subreddits'))
                else:
                    subreddit_id = f'u/{user_id}'

                submission_id = rch.get_submission_id(element)
                submission = rch.get_submission_elements(self.spider_name, subreddit_id,
                                                         submission_id)[0]
                submission_title = rch.get_submission_title(submission)
                submission_body = rch.get_submission_body(submission)
                yield f'{submission_title} {submission_body}'

            except Exception:
                self.logger.log(level='exception')

    def _get_user_comments(self, user_id):
        for element in rch.get_user_comments_elements(self.spider_name, user_id,
                                                      items_no=100)[::-1]:
            try:
                subreddit_id = rch.get_comment_subreddit_id(element)
                if subreddit_id:
                    self.send(Electron(value=subreddit_id, topic='subreddits'))

                comment_body = rch.get_comment_body(element)
                yield comment_body

            except Exception:
                self.logger.log(level='exception')

    def reject_request(self, context):
        time.sleep(5)
        self.rpc_notify('request_user', to='UserChooser')

    @rpc
    def put_user(self, context, user_id):
        if user_id not in self.seen_users:
            self.logger.log(f'Received VALID user {user_id} via RPC')
            self.users_ranking_queue.put(user_id)
            return
        self.logger.log(f'Received NOT VALID user {user_id} via RPC')
        self.rpc_notify('request_user', to='UserChooser')

    def _extract(self, user_id):
        self.logger.log(f'Extracting user: {user_id}')
        # Send all extracted texts as they are retrieved
        user_has_texts = False
        texts_counter = 0
        for text in self._get_user_texts(user_id):
            if text:
                electron = Electron(user_id, ('text', text))
                self.send(electron)
                user_has_texts = True
                texts_counter += 1
        self.logger.log(f'{texts_counter} texts extracted from the user {user_id}')

        # Send the End-of-User message
        if user_has_texts:
            electron = Electron(user_id, ('eou', None))
            self.send(electron)

    def extractor(self):
        running = True
        while (running):
            self.logger.log(f'User requested')
            self.rpc_notify('request_user', to='UserChooser')
            try:
                user_id = self.users_ranking_queue.get()
                if user_id not in self.seen_users:
                    self._extract(user_id)
                    self.seen_users.add(user_id)
            except Exception:
                self.logger.log(level='exception')


if __name__ == "__main__":
    UserExtractor(log_level='info', synchronous=True).start()
