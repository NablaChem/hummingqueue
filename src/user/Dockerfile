FROM python:3.10.7-slim
WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY ./ /code/
RUN pip install .
WORKDIR /code/hmq
CMD ["pytest", "--cov=./"]
