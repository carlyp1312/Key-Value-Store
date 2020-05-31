FROM python:3

WORKDIR /CSE138_Assignment4

RUN pip install Flask

RUN pip install requests

CMD ["python", "-u", "./api.py"]

COPY . .