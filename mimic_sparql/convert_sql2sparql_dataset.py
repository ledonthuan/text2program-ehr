import json
import os
import pandas as pd
import argparse
from rdflib import Graph
from collections import Counter

from mimicsql.evaluation.utils import query
from sql2sparql import SQL2SPARQL, split_entity
from evaluation_sparql import isequal
from build_mimicsparql_kg.build_complex_kg_from_mimicsqlstar_db import clean_text


def sparql_tokenize(sparql):
    sparql = split_entity(sparql)
    sparql = ' ^^'.join(sparql.split('^^'))
    sparql_tok = ' '.join(sparql.split(' '))
    return sparql_tok.split()


def convert_sql2sparql(complex=True, filename='train.json', dataset_type='natural', execution=True):
    if complex:
        savedir = f'./dataset/mimic_sparqlstar/{dataset_type}/'
        datadir = f'./dataset/mimicsqlstar/{dataset_type}/'

        sql2sparql = SQL2SPARQL(complex=complex, root='subject_id')

        if execution:
            print('LOAD  ... mimicqlstar.db')
            db_file = './build_mimicsqlstar_db/mimicsqlstar.db'
            model = query(db_file)
            print('DONE')

            print('LOAD KG ... mimic_kg')
            kg = Graph()
            kg.parse('./build_mimicsparql_kg/mimic_sparqlstar_kg.xml', format='xml', publicID='/')
            print('DONE')

    else:
        print(f'This dataset is Simple')
        savedir = f'./dataset/mimic_sparql/{dataset_type}/'
        datadir = f'./dataset/mimicsql/mimicsql_{dataset_type}/'

        sql2sparql = SQL2SPARQL(complex=complex, root='hadm_id')

        if execution:
            print('LOAD ... mimic.db')
            db_file = './mimicsql/evaluation/mimic_db/mimic.db'
            model = query(db_file)
            print('DONE')

            print('LOAD KG ... mimic_sparql_kg')
            kg = Graph()
            kg.parse('./build_mimicsparql_kg/mimic_sparql_kg.xml', format='xml', publicID='/')
            print('DONE')

    data = []
    with open(os.path.join(datadir, filename)) as json_file:
        for line in json_file:
            data.append(json.loads(line))

    df = pd.DataFrame(data)

    correct = 0
    sparqls = []
    for i, sql in enumerate(df['sql']):
        sql = sql.lower()
        sql_answer = []
        sparql_answer = []

        print("-" * 50)
        print(i, sql)

        if execution:
            sql_res = model.execute_sql(sql).fetchall()
            for res in sql_res:
                val = '|'
                temp = []
                for t in res:
                    val += str(t) + '|\t\t|'
                    temp.append(str(t))
                print(val[:-1])
                sql_answer.append(tuple(temp))
        print()

        sparql = sql2sparql.convert(sql)
        sparql = clean_text(sparql)

        print(i, sparql)
        if execution:
            sparql_res = kg.query(sparql)
            for res in sparql_res:
                val = '|'
                temp = []
                for t in res:
                    val += str(t.toPython()) + '|\t\t|'
                    temp.append(str(t.toPython()))
                print(val[:-1])
                sparql_answer.append(tuple(temp))

            print(sql_answer, sparql_answer, isequal(sql_answer, sparql_answer))
            if isequal(sql_answer, sparql_answer):
                correct += 1
            else:
                print("[incorrect]")
        print()

        sparql = sparql.lower()
        sparql_tok = sparql_tokenize(sparql)
        sparqls.append({'sql': sparql, 'sql_tok': sparql_tok})

    if execution:
        print(f'[SQL2SPARQL] filenmae: {filename}, Answer Accuracy: {correct/len(df):.4f}')

    sparql_data = []
    for d, sparql_d in zip(data, sparqls):
        d['sql'] = sparql_d['sql']
        d['sql_tok'] = sparql_d['sql_tok']
        sparql_data.append(d)

    save_filename = os.path.join(savedir, filename)
    with open(save_filename, 'w') as json_file:
        for dic in sparql_data:
            json.dump(dic, json_file)
            json_file.write('\n')

    print(f"Write to {save_filename}")


def build_vocab(complex=True, dataset_type='natural'):
    if complex:
        datadir = f'./dataset/mimic_sparqlstar/{dataset_type}'
    else:
        datadir = f'./dataset/mimic_sparql/{dataset_type}'

    filenames = ['train.json']
    counter = Counter()
    for filename in filenames:
        with open(os.path.join(datadir, filename)) as json_file:
            for line in json_file:
                dic = json.loads(line)
                counter.update(dic['question_refine_tok'])
                counter.update(dic['sql_tok'])

    with open(os.path.join(datadir, 'vocab'), 'w') as f:
        for k, v in counter.most_common():

            if len(k.split()) == 0:
                continue

            if k == ' ':
                continue
            f.write(f'{k} {v}\n')

    print(f'vocab builded: {len(counter)}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='mimicsql to mimic-sparql')
    parser.add_argument('--complex', default=False, type=lambda x: (str(x).lower() == 'true'))
    parser.add_argument('--dataset_type', type=str, default='natural', choices=['natural','template'])
    parser.add_argument('--execution', default=False, type=lambda x: (str(x).lower() == 'true'))
    args = parser.parse_args()

    execution = args.execution
    dataset_type = args.dataset_type
    complex = args.complex

    filenames = ['train.json', 'dev.json', 'test.json']
    for filename in filenames:
        convert_sql2sparql(complex=complex, filename=filename, dataset_type=dataset_type, execution=execution)
    build_vocab(complex=complex, dataset_type=dataset_type)
