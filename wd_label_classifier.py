#!/usr/bin/python3
import codecs
import csv

import numpy as np
import requests
import tensorflow as tf

query_train = 'select * { ?item wdt:P31 wd:Q4167836; p:P4224/pq:P106 ?param }'
classes = []
model_folder = None  # To reuse existing model specify path
query_unknown = 'select ?item { ?item wdt:P31 wd:Q4167836; p:P4224 ?s . ?s ps:P4224 wd:Q5; ?prop [] FILTER (?prop != pq:P21 ) FILTER NOT EXISTS {?item wdt:P971 []} } GROUP BY ?item HAVING(COUNT(*)=3)'
#languages = '"ru","uk","be","bg","kk","sr","ce",' \
#            '"ba","be-tarask","cv","os","ky","mrj","mk","mn","sah","tt","tg",' \
#            '"kbd","ab","av","bxr","crh","myv","xal","kv","lbe","lez","mdf","mhr","koi","krc","rue","tyv","udm",' \
#            '"cu","pnt","ady"'


languages = '"en","de","fr","nl","es","it","pl","ceb","sv","vi","war"'

def get_classes():
    result = [""]
    with requests.Session() as session:
        session.headers.update({'Accept': 'text/csv'})
        download = session.post('https://query.wikidata.org/sparql', params={
            'query': 'select ?class { [] p:P4224 ?s . ?s ps:P4224 wd:Q5; pq:P106 ?class } GROUP BY ?class ORDER BY DESC(COUNT(*)) LIMIT 300'
        })
        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        for row in my_list:
            row = [item.replace('http://www.wikidata.org/entity/', '') for item in row]
            if row[0].startswith('Q'):  # not header or exception
                result.append(row[0])

    return result


def tf_input_fn(data):
    result = {'label': tf.SparseTensor(indices=data[1], values=data[0],
                                       dense_shape=[np.amax(data[1], 0)[0] + 1, np.amax(data[1], 0)[1] + 1])}
    if len(data[2]) > 0:
        return result, tf.constant(data[2])
    else:
        return result


def query_labels_fn(query):
    offset = 0
    first = []
    second = None
    third = None

    with requests.Session() as session:
        session.headers.update({'Accept': 'text/csv'})

        while offset < 20000000:
            values = []
            indices = []
            y = []
            try:
                download = session.post('https://query.wikidata.org/sparql', params={
                    'query': 'SELECT ?item ?itemLabel ?param WITH {' + query + ' OFFSET ' + str(offset) +
                             ' LIMIT 50000} as %q {INCLUDE %q SERVICE wikibase:label {bd:serviceParam wikibase:language ' + languages + '}}'
                })
            except requests.exceptions.RequestException:
                break
            decoded_content = download.content.decode('utf-8')
            if 'exception' in decoded_content:
                print('timeout')

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            if len(my_list) == 1:  # no more data
                break

            for row in my_list:
                row = [item.replace('http://www.wikidata.org/entity/', '') for item in row]
                if len(row) == 3 and row[0].startswith('Q'):  # not header or exception
                    if row[1] != row[0]:  # actual label, not Qxxxxxx
                        words = row[1].translate(''.maketrans('",.)(:', '      ')).split()
                        if len(words) == 0:
                            continue

                        if row[2] == '':  # query for estimation
                            y.append(row[0].replace('Q', ''))  # store qid
                        else:
                            if row[2] in classes:
                                y.append(classes.index(row[2]))
                            else:
                                if classes[len(classes) - 1] == '':  # the rest of classes should be joined
                                    y.append(len(classes) - 1)
                                else:
                                    continue  # unknown class - skip

                        new_row_index = 1 + indices[len(indices) - 1][0] if len(indices) > 0 else \
                            1 + second[second.shape[0] - 1][0] if second is not None else 0
                        indices.extend([new_row_index, idx] for idx in range(len(words)))
                        values.extend(words)

            if len(values) > 0:
                third = np.array(y) if third is None else np.concatenate((third, y))
                second = np.array(indices) if second is None else np.concatenate((second, indices))
                first.extend(values)

            offset += 50000
            print(offset)

    return [first, second, third]

classes = get_classes()
tf.logging.set_verbosity(tf.logging.INFO)
m = tf.contrib.learn.LinearClassifier(
    feature_columns=[tf.contrib.layers.sparse_column_with_hash_bucket("label", hash_bucket_size=1000000)],
    model_dir=model_folder, n_classes=len(classes), config=tf.contrib.learn.RunConfig(keep_checkpoint_max=2),
    optimizer=tf.train.FtrlOptimizer(learning_rate=100, l1_regularization_strength=0.03)
)
if model_folder is None:
    m.fit(input_fn=lambda: tf_input_fn(query_labels_fn(query=query_train)), steps=10000)
unknown = query_labels_fn(query=query_unknown)
results = m.predict_proba(input_fn=lambda: tf_input_fn([unknown[0], unknown[1], []]))

with open("output.csv", 'wb') as o:
    o.write(codecs.BOM_UTF8)
    j = 0
    for i, p in enumerate(results):
        label = ''
        while True:
            label += str(unknown[0][j]) + ' '
            j += 1
            if j == len(unknown[0]):
                break
            if unknown[1][j - 1][0] < unknown[1][j][0]:
                break

        if classes[p.argmax()] == '':
            continue
        o.write(('Q' + str(unknown[2][i]) + ',' + classes[p.argmax()] + ',' +
                 str(p[p.argmax()]) + ',' + label + '\n').encode('utf-8'))
