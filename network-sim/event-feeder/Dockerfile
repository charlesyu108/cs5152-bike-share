FROM python:2.7-slim
WORKDIR /event-feeder
COPY ./event-feeder /event-feeder
COPY ./common /event-feeder
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 80
CMD ["python", "sim.py"]