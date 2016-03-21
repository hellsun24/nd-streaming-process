FROM centos:7

RUN yum install -q -y java-1.8.0-openjdk.x86_64 wget

RUN wget -nc -P /opt/ https://archive.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz

WORKDIR /opt/
RUN tar -xf kafka_2.10-0.8.2.2.tgz

RUN wget -nc -P /opt/ http://www.slf4j.org/dist/slf4j-1.7.6.tar.gz

WORKDIR /opt/
RUN tar -zxf slf4j-1.7.6.tar.gz

WORKDIR /opt/slf4j-1.7.6
RUN /bin/cp -rf slf4j-api-1.7.6.jar slf4j-log4j12-1.7.6.jar /opt/kafka_2.10-0.8.2.2/libs/

WORKDIR /opt/kafka_2.10-0.8.2.2/libs/
RUN rm -f /opt/kafka_2.10-0.8.2.2/libs/slf4j-log4j12-1.6.1.jar

RUN LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.71-2.b15.el7_2.x86_64/jre/lib/amd64/server/

COPY syslog-ng-premium-edition-5.6.1-1.rhel7.x86_64.rpm /opt/

WORKDIR /opt
RUN rpm -Uvf syslog-ng-premium-edition-5.6.1-1.rhel7.x86_64.rpm

COPY syslog-ng.conf /opt/syslog-ng/etc/
COPY license.txt /opt/syslog-ng/etc/
