import xml.etree.ElementTree as ET
import os
import random
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
import json
from tqdm import tqdm


path = './'
pos_folder = path + 'positive_examples_anonymous'
neg_folder = path + 'negative_examples_anonymous'
pos_files = os.listdir(pos_folder)
neg_files = os.listdir(neg_folder)

all = {'positive': [], 'negative': []}
folder = pos_folder
subject_type = 'positive'
for file in tqdm(pos_files + ['swap'] + neg_files):
    if file == 'swap':
        folder = neg_folder
        subject_type = 'negative'
        continue
    file_dict = {}
    tree = ET.parse(folder + '/' + file)
    root = tree.getroot()
    for child in root:
        if child.tag == 'ID':
            file_dict['id'] = child.text
            file_dict['texts'] = []
            continue
        text = None
        text = {'title': child[0].text.strip(),
                'date': child[1].text.strip(),
                'text': child[3].text.strip()}
        file_dict['texts'].append(text)
    all[subject_type].append(file_dict)

with open(path + 'training_set.json', 'w') as json_output:
    try:
        json.dump(all, json_output, indent=4)
    except Exception as e:
        print(e)
