#!/usr/bin/env python
# -*- coding: utf-8 -*-

from scipy.sparse.csr import csr_matrix
import pickle5 as pickle


class DepressionClassifier:
    def __init__(self):
        with open('binaries/lr_model', 'rb') as input_file:
            self.lr_model = pickle.load(input_file)
        with open('binaries/count_vectorizer', 'rb') as input_file:
            self.count_vectorizer = pickle.load(input_file)
        with open('binaries/tfidf_transformer', 'rb') as input_file:
            self.tfidf_transformer = pickle.load(input_file)

    def update_model(self, paths):
        with open(paths['lr_model'], 'rb') as input_file:
            self.lr_model = pickle.load(input_file)
        with open(paths['count_vectorizer'], 'rb') as input_file:
            self.count_vectorizer = pickle.load(input_file)
        with open(paths['tfidf_transformer'], 'rb') as input_file:
            self.tfidf_transformer = pickle.load(input_file)

    def vectorize(self, text):
        return self.count_vectorizer.transform([text])

    @staticmethod
    def aggregate_vector(vector1, vector2):
        return vector1 + vector2

    def tfidf(self, input_value):
        """ It accepts both text and count vector inputs """
        if type(input_value) == csr_matrix:
            return self.tfidf_transformer.transform(input_value)
        return self.tfidf(self.vectorize(input_value))

    def predict(self, input_value):
        """ It accepts both text and count vector inputs """
        if type(input_value) == list:
            tfidf_vector = self.tfidf(' '.join(input_value))
        elif type(input_value) == str or type(input_value) == csr_matrix:
            tfidf_vector = self.tfidf(input_value)
        else:
            tfidf_vector = input_value
        return self.lr_model.predict_proba(tfidf_vector).item(1)

    def aggregate_and_predict(self, text, previous_tfidf_vector=None):
        wordcount = self.vectorize(text)
        tfidf_vector = self.tfidf(wordcount)
        if previous_tfidf_vector:
            tfidf_vector = self.aggregate_vector(previous_tfidf_vector, tfidf_vector)
        return self.predict(tfidf_vector)
