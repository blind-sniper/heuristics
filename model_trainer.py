import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import LogisticRegression
from catenae import Link, Electron, utils
import logging
import json
import pickle5 as pickle
import time


class ModelTrainer(Link):

    NEW_MODEL_DFS_PATH = '/tmp/catenae/model_trainer'

    def setup(self):
        if len(self.args) < 1:
            self.logger.log('The threshold parameter was not provided.', level='error')
            self.suicide()

        self.threshold = float(self.args[0])
        if self.threshold == 0:
            self.logger.log('Pseudofeedback training is disabled.')
            self.suicide(exit_code=0)

        self.training_mode = 'all'
        valid_training_modes = ['all', 'positive', 'negative']
        if len(self.args) >= 2:
            training_mode = self.args[1]
            if training_mode in valid_training_modes:
                self.training_mode = training_mode

        self.logger.log(f'Threshold: {self.threshold}')
        self.logger.log(f'Training mode: {self.training_mode}')

        self.optimal_c = 16
        self.optimal_w = 4
        self.untagged_users = dict()
        self.user_probas = dict()

        with open('model_trainer/training_set.json') as json_input:
            self.training_set = json.load(json_input)
        self.training_set_sizes = (len(self.training_set['positive']), len(self.training_set['negative']))
        self.loop(self._train, interval=300)  # 5 min

    def _train(self):
        # Check if the training set has changed
        new_training_set_sizes = (len(self.training_set['positive']), len(self.training_set['negative']))
        if self.training_set_sizes[0] == new_training_set_sizes[0] \
        and self.training_set_sizes[1] == new_training_set_sizes[1]:
            self.logger.log('Training aborted, dataset unchanged.')
            return
        self.training_set_sizes = new_training_set_sizes

        corpus = []
        pos_counter = 0
        texts_counter = 0
        for user in self.training_set['positive'] + ['swap'] \
            + self.training_set['negative']:
            if user == 'swap':
                pos_counter = texts_counter
                continue
            user_text = ''
            for item in user['texts']:
                user_text += item['title'] + '\n' + item['text'] + '\n'
            corpus.append(user_text)
            texts_counter += 1

        y = np.zeros(texts_counter)
        y[:pos_counter] = 1
        count_vectorizer = CountVectorizer(min_df=20, stop_words='english')
        counts = count_vectorizer.fit_transform(corpus)
        tfidf_transformer = TfidfTransformer()
        x = tfidf_transformer.fit_transform(counts)
        x_array = x.toarray()
        train_y = y
        train_x = x_array
        lr_model = LogisticRegression(penalty='l1',
                                      solver='liblinear',
                                      C=self.optimal_c,
                                      class_weight={
                                          0: (1.0 / (1.0 + self.optimal_w)),
                                          1: (self.optimal_w / (1.0 + self.optimal_w))
                                      })
        lr_model = lr_model.fit(train_x, train_y)

        paths = {
            'lr_model': f'{ModelTrainer.NEW_MODEL_DFS_PATH}/lr_model',
            'count_vectorizer': f'{ModelTrainer.NEW_MODEL_DFS_PATH}/count_vectorizer',
            'tfidf_transformer': f'{ModelTrainer.NEW_MODEL_DFS_PATH}/tfidf_transformer'
        }

        with open(paths['lr_model'], 'wb') as binary_file:
            pickle.dump(lr_model, binary_file, protocol=pickle.HIGHEST_PROTOCOL)

        with open(paths['count_vectorizer'], 'wb') as binary_file:
            pickle.dump(count_vectorizer, binary_file, protocol=pickle.HIGHEST_PROTOCOL)

        with open(paths['tfidf_transformer'], 'wb') as binary_file:
            pickle.dump(tfidf_transformer, binary_file, protocol=pickle.HIGHEST_PROTOCOL)

        # Stats message
        electron = Electron(
            value={
                'event': 'model_trainer_trained_model',
                'timestamp': utils.get_timestamp_ms(),
                'value': {
                    'positive': self.training_set_sizes[0],
                    'negative': self.training_set_sizes[1]
                }
            })
        self.send(electron, topic='stats')
        self.rpc_call('UserUpdater', 'update_model', paths)
        self.rpc_call('BatchProbaUpdater', 'update_model', paths)
        self.logger.log(
            f'MODEL TRAINED - training set size: {self.training_set_sizes[0]} (pos), {self.training_set_sizes[1]} (neg)'
        )

    def _clean_user(self, user_id):
        del self.untagged_users[user_id]

    def add_user_proba(self, context, user_id, proba):
        self.logger.log(f'Proba received for user {user_id} ({proba})')
        self.user_probas[user_id] = proba

    def _wait_for_user_proba(self, user_id, attempts=10):
        if not user_id in self.user_probas:
            if attempts == 0:
                raise TimeoutError
            else:
                time.sleep(1)
                self._wait_for_user_proba(user_id, attempts - 1)

    def transform(self, electron):
        message_type, text = electron.value
        user_id = electron.key

        self.logger.log(f'Received input / type {message_type} / user_id {user_id}')

        if message_type != 'eou':
            if not user_id in self.untagged_users:
                self.untagged_users[user_id] = []
            self.untagged_users[user_id].append({'title': '', 'text': text})

        else:
            try:
                self._wait_for_user_proba(user_id)
                proba = self.user_probas[user_id]

                if proba >= 1 - self.threshold:
                    self.logger.log(f'positive user: {user_id} ({proba})')
                    if self.training_mode in ['all', 'positive']:
                        self.logger.log(f'ADDED ({user_id})', level='debug')
                        self.training_set['positive'].append({'id': user_id, 'texts': self.untagged_users[user_id]})

                elif proba <= self.threshold:
                    self.logger.log(f'negative user: {user_id} ({proba})')
                    if self.training_mode in ['all', 'negative']:
                        self.logger.log(f'ADDED ({user_id})', level='debug')
                        self.training_set['negative'].append({'id': user_id, 'texts': self.untagged_users[user_id]})

                else:
                    self.logger.log(f'dismissed user: {user_id} ({proba})')

            except TimeoutError:
                self.logger.log(f'skipping user {user_id}...', level='exception')
            except Exception:
                self.logger.log(level='exception')
            finally:
                self._clean_user(user_id)


if __name__ == "__main__":
    ModelTrainer(log_level='INFO', synchronous=True).start()
