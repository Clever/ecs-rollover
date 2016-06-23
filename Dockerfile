FROM python:2.7

RUN mkdir -p /opt/ecs-rollover
COPY requirements.txt /opt/ecs-rollover
RUN pip install -r /opt/ecs-rollover/requirements.txt

COPY src/__init__.py /opt/ecs-rollover/
COPY src/ec2.py /opt/ecs-rollover/
COPY src/ecs.py /opt/ecs-rollover/
COPY src/elb.py /opt/ecs-rollover/
COPY src/rollover.py /opt/ecs-rollover/
COPY src/scaling.py /opt/ecs-rollover/
COPY src/ssh.py /opt/ecs-rollover/
COPY src/utils.py /opt/ecs-rollover/

COPY src/entrypoint.sh /opt/ecs-rollover/

ENTRYPOINT ["/bin/bash", "/opt/ecs-rollover/entrypoint.sh"]
