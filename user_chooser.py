#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.mongodb import MongodbConnector
from catenae import (Link, Electron, utils, CircularOrderedSet, rpc)
import logging
import crawler_helper as rch
import time


class UserChooser(Link):
    def setup(self):
        # Small buffer (Mongo won't update inmediatly)
        self.seen_users = CircularOrderedSet(20)

        if len(self.args) > 0:
            self.ranking_mode = self.args[0]
            if len(self.args) >= 2:
                self.ranking_opts = self.args[1:]
            else:
                self.ranking_opts = ['desc']
        else:
            self.ranking_mode = 'proba'
            self.ranking_opts = ['desc']

        if self.ranking_mode not in [
                'random', 'proba', 'comments', 'points', 'hour', 'hierarchy', 'fusion'
        ]:
            raise ValueError('Unknown ranking mode')
        self.logger.log(f'Ranking mode: {self.ranking_mode}')
        if self.ranking_mode not in ['random', 'hierarchy', 'fusion']:
            self.logger.log(f'Ranking opts: {list(self.ranking_opts)}')

        self.mongodb.set_defaults('fuc_benchmark', 'users_ranking')

        self.loop(self.suicide, interval=3600, wait=True)

    def _pop_first_user_id(self):
        user_id = self._get_first_user(avoid=list(self.seen_users))

        # Mark the user as processed
        self.mongodb.update({'user_id': user_id}, {'processed': True})
        self.seen_users.add(user_id)
        return user_id

    @staticmethod
    def _swap_0_24(central_hour):
        if central_hour == 0:
            return 24
        if central_hour == 24:
            return 0
        return central_hour

    def _get_first_user(self, avoid=None):
        query = {'processed': {'$ne': True}}
        if avoid:
            query.update({'user_id': {'$nin': avoid}})

        # RANDOM
        if self.ranking_mode == 'random':
            result = self.mongodb.get_random(query=query)
            return next(result)['user_id']

        # FUSION
        if self.ranking_mode == 'fusion':
            # probadesc, commentsdesc
            merged_ranking = {}

            # probadesc top 100
            sort = [('proba.value', -1)]
            self.logger.log(f'sort: {sort}, query: {query}', level='debug')
            result = self.mongodb.get(query=query, sort=sort, limit=100)
            probadesc_items = 0
            for i, user in enumerate(result):
                probadesc_items = i
                merged_ranking[user['user_id']] = (i + 1, False)
            probadesc_items += 1

            # hour06 top 100
            sort = [('hour.dis06', 1)]
            self.logger.log(f'sort: {sort}, query: {query}', level='debug')
            result = self.mongodb.get(query=query, sort=sort, limit=100)
            hour06_items = 0
            for i, user in enumerate(result):
                hour06_items = i
                user_id = user['user_id']
                if user_id in merged_ranking:
                    merged_ranking[user_id] = ((merged_ranking[user_id][0] + i + 1) / 2.0, True)
                    continue
                merged_ranking[user_id] = (i + 1, False)
            hour06_items += 1

            # Average with the worst position
            max_value = min(100, probadesc_items, hour06_items)
            for key, value in merged_ranking.items():
                if not value[1]:
                    merged_ranking[key] = ((merged_ranking[user_id][0] + max_value) / 2.0, True)

            fusion_ranking = [{
                'user_id': key,
                'rank': value[0]
            } for key, value in merged_ranking.items()]
            fusion_ranking = sorted(fusion_ranking, key=lambda k: k['rank'], reverse=False)

            if fusion_ranking:
                return fusion_ranking[0]['user_id']
            return

        # HIERARCHY
        if self.ranking_mode == 'hierarchy':
            # PROBADESC, COMMENTSDESC, HOUR06
            sort = [('proba.value', -1)]
            self.logger.log(f'sort: {sort}, query: {query}', level='debug')
            result = self.mongodb.get(query=query, sort=sort, limit=2)
            user_list = [user for user in result]

            # Match for top 2 for probadesc, add hour06
            if len(user_list
                   ) == 2 and user_list[0]['proba']['value'] == user_list[1]['proba']['value']:
                self.logger.log('Match for PROBADESC in the top 2')
                electron = Electron(
                    value={
                        'event': 'user_chooser_hierarchy_match',
                        'timestamp': utils.get_timestamp_ms(),
                        'value': {
                            'ranking': 'probadesc'
                        }
                    })
                self.send(electron, topic='stats')

                proba = user_list[0]['proba']['value']

                query.update({'proba.value': proba})
                sort = [('proba.value', -1), ('hour.dis06', 1)]
                self.logger.log(f'sort: {sort}, query: {query}', level='debug')
                result = self.mongodb.get(query=query, sort=sort, limit=2)
                user_list = [user for user in result]

                # Match for top 2 for probadesc-hour06, add commentsdesc
                if len(user_list
                       ) == 2 and user_list[0]['hour']['dis06'] == user_list[1]['hour']['dis06']:
                    self.logger.log('Match for HOUR06 in the top 2')
                    electron = Electron(
                        value={
                            'event': 'user_chooser_hierarchy_match',
                            'timestamp': utils.get_timestamp_ms(),
                            'value': {
                                'ranking': 'hour06'
                            }
                        })
                    self.send(electron, topic='stats')

                    # Sort by comments.avg
                    user_list = sorted(user_list, key=lambda k: k['comments']['avg'],
                                       reverse=True)[:2]
                    self.logger.log('SELECTED by COMMENTSDESC')
                else:
                    self.logger.log('SELECTED by HOUR06')
                return user_list[0]['user_id']

            elif user_list:
                self.logger.log('SELECTED by PROBADESC')
                return user_list[0]['user_id']

            # Void ranking
            return

        order = -1
        if self.ranking_opts[0] == 'asc':
            order = 1

        # HOUR AVG
        if self.ranking_mode == 'hour':
            central_hour = int(self.ranking_opts[0])

            query.update({'hour.avg': {'$gte': central_hour}})
            sort = [('hour.avg', 1)]
            self.logger.log(f'sort: {sort}, query: {query}', level='debug')
            result = self.mongodb.get(query=query, sort=sort, limit=1)
            gte_result = next(result)

            # 24 if central_hour was 0
            central_hour = self._swap_0_24(central_hour)
            query.update({'hour.avg': {'$lte': central_hour}})
            sort = [('hour.avg', -1)]
            self.logger.log(f'sort: {sort}, query: {query}', level='debug')
            result = self.mongodb.get(query=query, sort=sort, limit=1)
            lte_result = next(result)

            if not gte_result and not lte_result:
                return
            elif gte_result and not lte_result:
                return gte_result['user_id']
            elif lte_result and not gte_result:
                return lte_result['user_id']

            # 0 if central_hour was 24
            central_hour = self._swap_0_24(central_hour)
            gte_distance = abs(1. * gte_result['hour']['avg'] - central_hour)

            # 24 if central_hour was 0
            central_hour = self._swap_0_24(central_hour)
            lte_distance = abs(1. * lte_result['hour']['avg'] - central_hour)

            if gte_distance >= lte_distance:
                return lte_result['user_id']
            return gte_result['user_id']

        # PROBA, COMMENTS, POINTS
        if self.ranking_mode in ['proba', 'comments', 'points']:
            if self.ranking_mode == 'proba':
                property_ = 'proba.value'
            else:
                property_ = self.ranking_mode + '.avg'
            sort = [(property_, order)]
            self.logger.log(f'sort: {sort}, query: {query}', level='debug')
            result = self.mongodb.get(query=query, sort=sort, limit=1)
            return next(result)['user_id']

    @rpc
    def request_user(self, context, attempts=10):
        user_extractor = context['uid']
        if attempts != 10:
            self.logger.log(f'Attempt {10 - attempts} for extractor {user_extractor}')
        else:
            self.logger.log(f'User request received from extractor {user_extractor}')

        try:
            user_id = self._pop_first_user_id()
            if user_id == None:
                raise ValueError
            self.logger.log(f'Selected user: {user_id} for extractor {user_extractor}')
            self.rpc_notify('put_user', user_id, to=user_extractor)

        except Exception:
            self.logger.log(level='debug')
            self.logger.log(f'Cannot retrieve an user for extractor {user_extractor}', level='warn')
            if attempts > 0:
                time.sleep(0.5)
                self.request_user(context, attempts - 1)
            else:
                self.rpc_notify('reject_request', to=user_extractor)


if __name__ == "__main__":
    UserChooser(log_level='info', synchronous=True).start()
