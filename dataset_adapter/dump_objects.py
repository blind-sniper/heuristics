import xml.etree.ElementTree as ET
import os
import numpy as np
import sys
import random
import socket
import scipy
import gensim
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.feature_selection import VarianceThreshold
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
from sklearn.externals import joblib
import pickle5 as pickle

### MAIN PROGRAM

random.seed(1234)
np.random.seed(1234)

### FIRST STEP. READ XMLs and concatenate all text written by the subjects

optimalC = 16
optimalw = 4

corpus = []

path = './'

pos_folder = path + 'positive_examples_anonymous'
neg_folder = path + 'negative_examples_anonymous'

listfiles = os.listdir(pos_folder)
listfiles2 = os.listdir(neg_folder)

# stores the number of writings for each user
nwritings = np.zeros([len(listfiles) + len(listfiles2)])
index = 0

for file in listfiles:
    filepath = os.path.join(pos_folder, file)
    #print((filepath))
    tree = ET.parse(filepath)
    root = tree.getroot()
    #prints ID
    #print((root[0].text))
    all_text = ''
    for child in root:
        if child.tag != 'ID':
            # if child is not ID then it has to be a writing element
            # child 0 is title, 1 is date, 2 is info and 3 is text
            # print((child[0].text))
            # print((child[3].text))
            # print((" "))
            all_text = all_text + '\n' + child[0].text + '\n' + child[3].text
            nwritings[index] = nwritings[index] + 1

    corpus.append(all_text)
    index = index + 1

print(("Positive Set:%d users processed" % len(listfiles)))

for file in listfiles2:
    filepath = os.path.join(neg_folder, file)
    #print((filepath))
    tree = ET.parse(filepath)
    root = tree.getroot()
    #prints ID
    #print((root[0].text))
    all_text = ''
    for child in root:
        if child.tag != 'ID':
            # if child is not ID then it has to be a writing element
            # child 0 is title, 1 is date, 2 is info and 3 is text
            # print((child[0].text))
            # print((child[3].text))
            # print((" "))
            all_text = all_text + '\n' + child[0].text + '\n' + child[3].text
            nwritings[index] = nwritings[index] + 1

    corpus.append(all_text)
    index = index + 1

print("Negative Set:%d users processed" % len(listfiles2))

print("Writings per user: min %f, max: %f, mean: %f" %
      (nwritings.min(), nwritings.max(), nwritings.mean()))

#vectorizer = TfidfVectorizer(min_df=20,stop_words='english')
#X = vectorizer.fit_transform(corpus)

count_vectorizer = CountVectorizer(min_df=20, stop_words='english')
counts = count_vectorizer.fit_transform(corpus)
vectorizer = TfidfTransformer()
X = vectorizer.fit_transform(counts)

Xarray = X.toarray()

print("\nCollection vectorized. %d rows and %d cols." % Xarray.shape)

# now we create the vector of responses
y = np.zeros([len(listfiles) + len(listfiles2)])
y[0:len(listfiles)] = 1

trainy = y
trainx = Xarray

print(
    "\nApplying C and class weight learned by cross validation to all the training set..."
)

lr = LogisticRegression(
    penalty='l1',
    solver='liblinear',
    C=optimalC,
    class_weight={
        0: (1.0 / (1.0 + optimalw)),
        1: (optimalw / (1.0 + optimalw))
    })
lr = lr.fit(trainx, trainy)

with open('../binaries/lr_model', 'wb') as binary_file:
    pickle.dump(lr, binary_file, protocol=pickle.HIGHEST_PROTOCOL)

with open('../binaries/count_vectorizer', 'wb') as binary_file:
    pickle.dump(
        count_vectorizer, binary_file, protocol=pickle.HIGHEST_PROTOCOL)

with open('../binaries/tfidf_transformer', 'wb') as binary_file:
    pickle.dump(vectorizer, binary_file, protocol=pickle.HIGHEST_PROTOCOL)
