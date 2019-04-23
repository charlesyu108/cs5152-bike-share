from flask import Flask, request, jsonify, session
from sklearn.externals import joblib
import traceback
import pandas as pd

# https://medium.freecodecamp.org/how-to-build-a-web-application-using-flask-and-deploy-it-to-the-cloud-3551c985e492
# https://programminghistorian.org/en/lessons/creating-apis-with-python-and-flask#creating-a-basic-flask-application

# working with ML model
# https://www.datacamp.com/community/tutorials/machine-learning-models-api-python

global prediction_model 
prediction_model = None

app = Flask(__name__)
app.secret_key = 'bichael'

# IGNORE FOR NOW
# request: event from alert-users function server
# response: send to current loaded prediction model and return result
@app.route('/predict', methods=['POST'])
def prediction():

	try:
		if 'model' not in session:
			print("model is not stored in session")
			fn = "model.pkl"
		else:
			fn = session.get('model')

		print(fn)
		lr = joblib.load(fn)
		print(lr)
		model_columns = joblib.load("model_columns.pkl")
		print(model_columns)
		# request must have content-type of json, otherwise will get None
		json_ = request.get_json()
		print(json_)
		query = pd.get_dummies(pd.DataFrame(json_))
		print(query)
		query = query.reindex(columns=model_columns, fill_value=0)
		print(query)
		prediction = list(lr.predict(query))
		print(prediction)
		return jsonify({'prediction': prediction})
	except:
		# error
		return jsonify({'trace': traceback.format_exc()})

# request: new prediction model to be loaded from ML model as json
@app.route('/model', methods=['POST'])
def newModel():
	# file = request.files['file']
	# # save file if not already saved in same directory
	# #file.save('/' + secure_filename(file.filename))

	# if file:
	# 	session['model'] = file.filename
	# 	print(session['model'])
	# 	return "TRUE"

	# return "FALSE"
	
	global prediction_model
	prediction_model = request.get_json()
	return "OK"

@app.route('/getPrediction', methods=['GET'])
def getPrediction():
	global prediction_model
	return prediction_model

if __name__ == "__main__":
	#prediction_model = joblib.load("model.pkl")
	# app.config.from_mapping(
	# 	SECRET_KEY = 'bichael' # change later
	# )
	app.run(debug=True)
