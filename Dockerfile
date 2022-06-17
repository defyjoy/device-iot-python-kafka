FROM python:alpine
WORKDIR /app
ENV PYTHONUNBUFFERED 1
COPY main.py /app/
COPY requirements.txt /app/

RUN mkdir /app/devicesensor
COPY ./devicesensor /app/devicesensor/

RUN apk --update --upgrade add gcc musl-dev jpeg-dev zlib-dev libffi-dev cairo-dev pango-dev gdk-pixbuf-dev
RUN pip install -r requirements.txt

RUN export PYTHONPATH=/app/

CMD ["python", "main.py"]