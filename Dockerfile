FROM python:3

WORKDIR /CSE138_Assignment2

RUN pip install Flask

# RUN pip install docker

RUN pip install requests

CMD ["python", "-u", "./api.py"]

COPY . .

# ENV FORWARDING_ADDRESS = ${var}
# RUN echo "VAR = ${var}"

#RUN if [FORWARDING_ADDRESS = ""]; then FORWARDING_ADDRESS; else MAIN_ADDRESS

#ENV env_var_name=$name
#if (FORWARDING_ADDRESS =='forwarding-container')
    ##{ENV=TRUE}
#else:
    #{ENV=FALSE}