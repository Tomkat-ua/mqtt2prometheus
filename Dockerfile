FROM python:3-slim

WORKDIR /usr/src/app

#RUN apt-get update && apt-get install -y  libmariadbclient-dev   && rm -rf /var/lib/apt/lists/*```

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY ./config.yml /usr/src/app/config.yml
COPY ./main.py /usr/src/app/main.py

CMD ["python","-u","main.py"]
