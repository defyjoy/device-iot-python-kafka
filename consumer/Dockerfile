FROM python:slim-bullseye
WORKDIR /app
ENV PYTHONUNBUFFERED 1
COPY main.py /app/
COPY requirements.txt /app/

RUN mkdir /app/sensor
COPY ./sensor /app/sensor/

# RUN apk --update --upgrade add gcc musl-dev jpeg-dev zlib-dev libffi-dev cairo-dev pango-dev gdk-pixbuf-dev g++
RUN apt-get update && apt install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget


# RUN apk add --no-cache tzdata && cp -r -f /usr/share/zoneinfo/Asia/Kolkata /etc/localtime
RUN pip install -r requirements.txt
RUN export PYTHONPATH=/app/

CMD ["python", "main.py"]