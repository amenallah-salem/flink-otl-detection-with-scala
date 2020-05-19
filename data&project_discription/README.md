# Exercice prepared for Paris-Dauphine Streaming Module!

In this repository, you will find all exercices needed for this module
To start, we first need to setup some Docker images.
Docker will help us easily install all needed dependencies.

# Docker

## Install Docker

Please follow instructions here : [https://docs.docker.com/v17.09/engine/installation/](https://docs.docker.com/v17.09/engine/installation/)

## Startup Kafka Cluster

Should be as simple as :
```
$> cd flink-exercices/docker/kafka-zk
$> docker-compose up -d
```
To check everything is working fine, you can go to `localhost:9000` and you should see a UI.
Now Kafka/Zookeeper are running in background.

## Read kafka topics using CLI

Now you have a Kafka cluster running, let's try to read from some topics.
`clicks` and `displays` topics should have been automatically created from previous step.
We want to read those topics from our machine (not from a docker container).
Depending on your OS, Kafka is either running on a VM (using docker-machine) or directly in your host.
In the following steps, I'm assuming docker is running directly on your host.
If you use a vm, then we need to update few variables.
TODO....

### Download Kafka scripts
Download https://archive.apache.org/dist/kafka/2.0.1/kafka_2.11-2.0.1.tgz
Then extract the file :
```
$> tar xf kafka_2.11-2.0.1.tgz
$> cd kafka_2.11-2.0.1/bin
$> kafkacat -b localhost:9092 -t clicks
```

You will notice in the python script we're targeting port 9293 since it's running from a container
Cf. kafka cluster config

### Inject events to Kafka

As seen in previous step, Kafka clicks topic is empty, let's generate some events and read them :
In another shell :
```
$> cd flink-exercices/docker/generate_kafka_events
$> docker build -t python-kafka-generate . 
```
This will build a simple image with python-kafka dependency.
```
$> docker run -it --network kafka-zk_bridge --rm  -v "$PWD":/usr/src/myapp -w /usr/src/myapp python-kafka-generate python generator.py
```

Now, if you switch back to `kafkacat` shell, you should see many events!
