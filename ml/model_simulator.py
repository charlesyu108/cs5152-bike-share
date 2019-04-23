import csv
import datetime
import time
import random
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from multiprocessing import Queue, Process
from threading import Thread, Lock
import pickle
from collections import defaultdict

infile = open('station_cols', 'rb')
columns = pickle.load(infile)
infile = open('model', 'rb')
model = pickle.load(infile)


def simulator():
    trip_data = pd.read_csv('trip.csv')
    stations = trip_data.station_number.unique()
    counts = np.zeros((121, 1))
    counts = np.insert(counts, 0, stations, axis=1)
    zero_prev = pd.DataFrame(counts, columns=['station', 'count'])
    zero_prev.set_index(['station'], inplace=True)
    for row in counts:
        row[1] = 1
    counts_df = pd.DataFrame(counts, columns=['station', 'count'])
    counts_df.set_index(['station'], inplace=True)
    model_counts = pd.DataFrame(counts, columns=['station', 'count'])
    model_counts.set_index(['station'], inplace=True)
    failures = 0
    model_failures = 0
    samples = trip_data.sample(100)
    prev = {int(num): [0] for num in stations}
    prev['time'] = 0
    prev = pd.DataFrame.from_dict(prev)
    for i, trip in samples.iterrows():
        stn = trip.station_number
        if trip.is_arrival:

            counts_df.loc[stn]['count'] = counts_df.loc[stn]['count'] + 1

            # assume the suggestion is accepted with 25% probability
            # if(random.random() <= 0.5):
            prev['time'] = trip.time
            print(prev)
            stn = model.predict(prev)

            model_counts.loc[stn]['count'] = model_counts.loc[stn]['count'] + 1
            prev = zero_prev
            prev.loc[stn] = 1
            prev = prev.T

        else:
            # print(counts_df)
            if counts_df.loc[stn]['count'] == 0:
                failures = failures + 1
            else:
                counts_df.loc[stn]['count'] = counts_df.loc[stn]['count'] - 1
            if model_counts.loc[stn]['count'] == 0:
                model_failures = model_failures + 1
            else:
                model_counts.loc[stn]['count'] = model_counts.loc[stn]['count'] - 1

    print("Original failure rate of {}%".format(failures/100))
    print("Model failure rate of {}%".format(model_failures/100))


if __name__ == '__main__':
    simulator()
