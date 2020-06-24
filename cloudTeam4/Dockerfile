FROM python:3.7-slim 

RUN pip install boto3==1.9.6
COPY ./requirements.txt .
RUN pip install --trusted-host pypi.python.org -r requirements.txt
RUN pip install Flask-RESTful requests redis

COPY ./code/ /opt/app   
COPY ./files/ /opt/app/files              
WORKDIR /opt/app
CMD ["python", "namenode.py"]