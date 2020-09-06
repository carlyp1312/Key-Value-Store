FROM python:3

WORKDIR /Key-Value-Store

RUN pip install Flask

RUN pip install requests

CMD ["python", "-u", "./api.py"]

COPY . .
