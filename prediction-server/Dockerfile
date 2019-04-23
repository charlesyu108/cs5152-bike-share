FROM python:3.7

COPY . /app
WORKDIR /app

EXPOSE 80
RUN pip install flask

ENTRYPOINT python prediction-flask.py 

