FROM python:3

ADD util.py /
ADD main.py /
ADD common.conf /
ADD etl_lock /

RUN pip install boto3
RUN pip install botocore
RUN pip install configparser
RUN pip install pandas
#RUN pip install io
#RUN pip install atexit
#RUN pip install logging
RUN pip install datetime
#RUN pip install sys
RUN pip install pyarrow
RUN pip install fastparquet

CMD [ "python3", "./main.py" ]


