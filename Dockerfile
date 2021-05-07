# syntax=docker/dockerfile:1

FROM python:3.8

ADD main.py .

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt


CMD [ "python3", "-m" , "./main.py"]