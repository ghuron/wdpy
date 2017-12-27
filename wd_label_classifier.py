#!/usr/bin/python3
import csv

import numpy as np
import requests
import tensorflow as tf

query_train = '?item wdt:P31 ?param'  # '?item wdt:P31 wd:Q5; wdt:P21 ?param'
query_unknown = '?item wdt:P18 ?i . OPTIONAL {?item wdt:P31 ?f} FILTER(!bound(?f)) . ' + \
                'OPTIONAL {?item wdt:P279 ?s} FILTER(!bound(?s))'
# '?item wdt:P31 wd:Q5 OPTIONAL {?item wdt:P21 ?f} FILTER(!bound(?f))'
classes = ['Q11266439', 'Q4167836', 'Q15184295', 'Q16521', 'Q11173', 'Q5', 'Q56061', '']  # ['Q6581097', 'Q6581072']


def tf_input_fn(data):
    result = {'label': tf.SparseTensor(indices=data[1], values=data[0],
                                       dense_shape=[np.amax(data[1], 0)[0] + 1, np.amax(data[1], 0)[1] + 1])}
    if len(data[2]) > 0:
        return result, tf.constant(data[2])
    else:
        return result


def query_labels_fn(query_filter):
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
            download = session.post('https://query.wikidata.org/sparql', params={
                'query': 'SELECT ?item ?itemLabel ?param WITH {SELECT * {' + query_filter + '} OFFSET ' + str(offset) +
                         ' LIMIT 50000' +
                         '} as %q {INCLUDE %q SERVICE wikibase:label {bd:serviceParam wikibase:language "ru"}}'
            })
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
                        words = row[1].translate(''.maketrans('",.)(', '     ')).split()
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


tf.logging.set_verbosity(tf.logging.INFO)

m = tf.contrib.learn.LinearClassifier(
    feature_columns=[tf.contrib.layers.sparse_column_with_hash_bucket("label", hash_bucket_size=200000)],
    optimizer=tf.train.FtrlOptimizer(learning_rate=100, l1_regularization_strength=0.001)
    # , model_dir='C:\\Users\\Ghuron\\AppData\\Local\\Temp\\tmpzy8ujk08\\'
)
m.fit(input_fn=lambda: tf_input_fn(query_labels_fn(query_filter=query_train)), steps=10000)

unknown = query_labels_fn(query_filter=query_unknown)
results = m.predict_proba(input_fn=lambda: tf_input_fn([unknown[0], unknown[1], []]))
j = 0
with open("output.csv", 'wb') as o:
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
