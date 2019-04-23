FROM python:3.7

COPY . /ml
WORKDIR /ml

RUN pip install -r requirements.txt
RUN /bin/bash -c "source .env"

ENTRYPOINT python ml_pull_queue.py
