FROM python:3.8

RUN mkdir -p /usr/src/functions_api/
WORKDIR /usr/src/functions_api/


COPY . /usr/src/functions_api/
RUN pip install --no-cache-dir -r requirements.txt
RUN sed -i 's/\r$//' ./run.sh

RUN apt update && \
    apt install -y locales && \
    sed -i -e 's/# ru_RU.UTF-8 UTF-8/ru_RU.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales

ENV LANG ru_RU.UTF-8
ENV LC_ALL ru_RU.UTF-8

EXPOSE 8080

RUN chmod a+x ./run.sh

CMD ["/bin/bash", "./run.sh"]
