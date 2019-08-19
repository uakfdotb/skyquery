import pyramid.arima

import json
import numpy
import sys

with open(sys.argv[1], 'r') as f:
	query = json.load(f)

def get_predictions(series):
	series = numpy.array(series, dtype='float32')
	model = pyramid.arima.auto_arima(series, m=96, seasonal=True, d=1, D=1)
	predictions, confidence = model.predict(n_periods=672, return_conf_int=True)
	print 'series: ', series
	print 'predictions: ', predictions
	print 'confidence: ', confidence

for series in query:
	get_predictions(series)
