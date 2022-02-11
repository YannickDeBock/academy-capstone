FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
WORKDIR /workspace/academy-capstone
COPY requirements.txt /workspace/academy-capstone
USER root
RUN python3 -m pip install -r requirements.txt
COPY __init__.py /workspace/academy-capstone
COPY snowflake_secret.py /workspace/academy-capstone
COPY ETL.py /workspace/academy-capstone
CMD python3 ETL.py