import pickle
import json
import pandas as pd
from os.path import getsize

with open('data/contributors_sample.json', 'r') as f:
    data_json = json.load(f)

jobs_df = pd.DataFrame(data_json, columns=['jobs', 'username']).explode(column=['jobs']).groupby('jobs')
jobs = {}


def ex1(data):
    for job, username in zip(data['jobs'], data['username']):
        jobs.setdefault(job, [])
        jobs[job].append(username)


jobs_df.apply(ex1)


def ex2(data):
    with open('data/jobs_usernames.pickle', 'wb') as pickle_f:
        pickle.dump(data, pickle_f)

    with open('data/jobs_usernames.json', 'w') as json_f:
        json.dump(data, json_f)

    return f'pickle: {getsize("data/jobs_usernames.pickle")} Б\njson: {getsize("data/jobs_usernames.json")} Б'


def ex3(file):
    with open(file, 'rb') as f:
        data = pickle.load(f)
    return data


print(ex2(jobs))
print(ex3('data/jobs_usernames.pickle'))
