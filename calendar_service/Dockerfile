FROM python:3.8

RUN mkdir -p /usr/src/calendar_service/
WORKDIR /usr/src/calendar_service/

COPY . /usr/src/calendar_service/
RUN pip install --no-cache-dir -r requirements.txt
RUN sed -i 's/\r$//' ./run.sh

EXPOSE 80

RUN chmod a+x ./run.sh

CMD ["/bin/bash", "./run.sh"]
