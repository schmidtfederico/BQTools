FROM python:2

RUN mkdir /code
WORKDIR /code

ADD ./docker/pip_requirements.txt /code/
RUN pip install -r pip_requirements.txt

ADD . /code/

COPY ./docker/entrypoints/ /code/