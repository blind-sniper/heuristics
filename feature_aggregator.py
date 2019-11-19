#!/usr/bin/env python
# -*- coding: utf-8 -*-

from numpy import average
from datetime import datetime
from depression_classifier import DepressionClassifier
import logging
from catenae import utils


class FeatureAggregator:
    def __init__(self):
        self.classifier = DepressionClassifier()

    @staticmethod
    def get_dict_value(key, dict_):
        if dict_ and key in dict_:
            return dict_[key]

    @staticmethod
    def get_comments(measures, comments):
        return FeatureAggregator.get_avg_measure('comments', measures, comments)

    @staticmethod
    def get_points(measures, points):
        return FeatureAggregator.get_avg_measure('points', measures, points)

    @staticmethod
    def get_hour(measures, timestamp):
        dt = datetime.fromtimestamp(timestamp)
        decimal_hour = dt.hour + dt.minute / 60
        hour_dict = FeatureAggregator.get_avg_measure('hour', measures, decimal_hour)

        # Add distances
        for hour in [0, 6, 12, 18]:
            hour_prefix = ''
            if hour < 10:
                hour_prefix = '0'
            key = f'dis{hour_prefix}{hour}'
            # min(abs(h1 - h2), 24 - abs(h1 - h2))
            first_dis = abs(hour_dict['hour']['avg'] - hour)
            hour_dict['hour'][key] = min(first_dis, 24 - first_dis)

        return hour_dict

    def get_proba(self, measures, text):
        proba_dict_value = self.get_dict_value('proba', measures)
        if proba_dict_value:
            texts = proba_dict_value['texts']
            texts.append(text)
        else:
            texts = [text]
        proba = self.classifier.predict(texts)
        return {'proba': {'value': proba, 'texts': texts}}

    @staticmethod
    def get_avg_measure(attribute, measures, new_measure):
        attribute_dict_value = FeatureAggregator.get_dict_value(attribute, measures)

        if not attribute_dict_value:
            return {attribute: {'avg': new_measure, 'count': 1}}

        avg = float(attribute_dict_value['avg'])
        count = attribute_dict_value['count']

        try:
            avg = average([avg, new_measure], weights=[count / (count + 1), 1 / (count + 1)])
        except Exception:
            logging.exception('')

        count += 1
        return {attribute: {'avg': avg, 'count': count}}
