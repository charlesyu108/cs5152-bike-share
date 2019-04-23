from flask import Flask, request, jsonify, session

global prediction_model 
prediction_model = None

app = Flask(__name__)

@app.route('/model', methods=['POST'])
def newModel():
	
	global prediction_model
	prediction_model = request.get_json()
	return "OK"

@app.route('/getPrediction', methods=['GET'])
def getPrediction():
	global prediction_model
	return str(prediction_model)

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0', port=80)
