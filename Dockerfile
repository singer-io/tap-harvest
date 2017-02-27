FROM alpine:3.4

RUN mkdir /code
WORKDIR /code
ADD . /code

RUN apk update
RUN apk upgrade
RUN apk add curl
RUN apk add python3
RUN pip3 install --upgrade pip setuptools && \
    rm -r /root/.cache
    
RUN pip3 install -r requirements.txt
RUN pip3 install persist-stitch==0.2.3

CMD ["persist-stitch", "python3", "stream_github.py"]
