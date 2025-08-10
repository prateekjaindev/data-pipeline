FROM flink:1.17.1-scala_2.12

# For Python processor
RUN apt-get update && apt-get install -y python3 python3-pip

# Copy Python requirements and install
COPY requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt

# Copy Python processor
COPY python_processor.py /opt/flink/usrlib/

# Copy Java JAR (when built)
# COPY target/ico-flink-processor-1.0-SNAPSHOT.jar /opt/flink/usrlib/

# Copy log configuration
COPY log4j2.properties /opt/flink/conf/