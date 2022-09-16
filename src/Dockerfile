FROM --platform=linux/amd64 public.ecr.aws/amazonlinux/amazonlinux:2.0.20220207.1

RUN yum update -y && \
	yum install python3 -y && \
	yum install unzip -y

# install aws cli
RUN /usr/bin/curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" &&\
	/usr/bin/unzip awscliv2.zip &&\
	./aws/install && \
	rm awscliv2.zip

# boto 3
RUN /usr/bin/pip3 install boto3 && \
    /usr/bin/pip3 install awswrangler && \
	/usr/bin/pip3 install Jinja2

# cleanup
RUN yum clean all && \
	rm -rf /var/cache/yum

COPY materialize_athena_query.py .
COPY sql_jobs/ sql_jobs/