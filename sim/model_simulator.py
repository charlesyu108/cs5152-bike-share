import csv, datetime, time, random
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
	counts = np.zeros((121,1))
	counts = np.insert(counts, 0, stations, axis=1)
	zero_prev = pd.DataFrame(numy.rot90(counts), rows=['station', 'count'])
	prev.set_index(['station'])
	for row in counts:
		row[1] = 25
	counts_df = pd.DataFrame(counts, columns=['station', 'count'])
	counts_df.set_index(['station'])
	model_counts = pd.DataFrame(counts, columns=['station', 'count'])
	model_counts.set_index(['station'])
	failures = 0
	model_failures = 0
	samples = trip_data.samples(1000)
	prev = zero_prev
	for trip in samples.iterrows():
		stn = trip.station_number
		if trip.is_arrival:
			
			counts_df.loc[stn]['count'] = counts_df.loc[stn]['count'] +1

			# assume the suggestion is accepted with 25% probability
			if(random.random() <= 0.25):
				stn = rfr.predict(prev)
			
			model_counts.loc[stn]['count'] = model_counts.loc[stn]['count'] +1
			prev = zero_prev
			prev.loc[stn] = 1

		else:
			if counts_df.loc[stn]['count'] == 0:
				failures = failures +1
			if model_counts.loc[stn]['count'] == 0:
				model_failures = model_failures +1
			counts_df.loc[stn]['count'] = counts_df.loc[stn]['count'] -1
			model_counts.loc[stn]['count'] = model_counts.loc[stn]['count'] -1
			prev = zero_prev
			prev.loc[stn] = -1        

	print("Original failure rate of %d \% \n", failures/1000)
	print("Model failure rate of %d \% \n", model_failures/1000)
				
			
				
		
		


