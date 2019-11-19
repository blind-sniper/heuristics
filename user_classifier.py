#!/usr/bin/env python
# -*- coding: utf-8 -*-

from depression_classifier import DepressionClassifier
from catenae import Link, Electron, utils
import logging


class UserClassifier(Link):
    def setup(self):
        self.classifier = DepressionClassifier()
        self.wordcounts = {}
        self.counts = {}

    def send_user_prediction_event(self, user_id, proba, count):
        electron = Electron(value={
            'event': 'user_classifier_user_prediction',
            'timestamp': utils.get_timestamp_ms(),
            'value': {
                'user_id': user_id,
                'proba': proba,
                'count': count
            }
        },
                            topic='stats')
        self.send(electron)

    def transform(self, electron):
        message_type, text = electron.value
        user_id = electron.key

        if message_type == 'eou' and user_id in self.wordcounts:
            wordcount = self.wordcounts.pop(user_id)
            proba = self.classifier.predict(wordcount)
            self.rpc_call('ModelTrainer', 'add_user_proba', [user_id, proba])

            count = self.counts.pop(user_id)
            self.send_user_prediction_event(user_id, proba, count)
            self.logger.log(f'+ User {user_id} classified')
            
        else:
            wordcount = self.classifier.vectorize(text)
            if user_id in self.wordcounts:
                wordcount = self.classifier.aggregate_vector(wordcount, self.wordcounts[user_id])

            self.wordcounts[user_id] = wordcount
            self.counts[user_id] = self.counts[user_id] + 1 \
                if user_id in self.counts else 1
            self.logger.log(f'Added text from {user_id}', level='debug')


if __name__ == "__main__":
    UserClassifier(log_level='info', synchronous=True).start()
