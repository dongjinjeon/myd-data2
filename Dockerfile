FROM tiangolo/uwsgi-nginx-flask:python3.9

ENV LISTEN_PORT 9090

EXPOSE 9090

COPY . /app

RUN pip install -r requirements.txt

