# Data Pipe

## Goal : Reading streaming data from rabbbitmq or kafka and writing the data to another endpoint and add Custom Logic if needed

1. Property File : src/main/resources/*.Properties
2. Custom Logic  : File com.symantec.cpe.storm.LogicBolt -> Add any custom logic in execute method, We will add different spouts down the line 


## Building project 

1. Download this project(rabbitmq spout and ) https://github.com/ppat/storm-rabbitmq  and install the jar
2. mvn install:install-file -Dfile=${WORKSPACE}/${PROJECT}/storm-rabbitmq-0.6.2-SNAPSHOT.jar -DgroupId=io.latent -DartifactId=storm-rabbitmq  -Dversion=0.6.2-SNAPSHOT -Dpackaging=jar
3. mvn clean test package -U

## Deploying project 

1. Update the property File
2. Add any logic if required in the Logic Bolt , build Jar as per above
3. Run as per the below command

## Run Options 
Local mode within eclipse IDE or Remote to any cluster

```sh
eg : storm jar  data-pipe-storm-0.0.2-SNAPSHOT-jar-with-dependencies.jar -c nimbus.host=<hostname> -c nimbus.port=<port_number> com.symantec.cpe.StartService <PropertyFile>
eg : storm jar  data-pipe-storm-0.0.2-SNAPSHOT-jar-with-dependencies.jar com.symantec.cpe.StartService <PropertyFile>
```


## Kafka Input Parameters
```sh
Property          	Type      (format)Example
streamName         String     datapipe   <reused as group.id Consumers>
sourceZooKeeperURL String     localhost:2181
inputTopic         String     source-ga 
 ```

## RabbitMQ Input Parameters
```sh
Property          	Type      (format)Example
rabbitmq.host    	String	localhost
rabbitmq.port		Integer	5672 (default)
rabbitmq.username	String	admin
rabbitmq.password	String	admin
rabbitmq.prefetchCount	Integer	200
rabbitmq.queueName	String	hello
rabbitmq.requeueOnFail	Boolean	true
 ```

## Kafka Output Parameters
```sh
Property					Type (format)	Example											Comment
destinationKafkaURL			String			localhost\:6667   		Comma-separated list of all Kafka brokers at the destination cluster.
outputTopic					String			Kafka_Replication
destinationZooKeeperURL		String			localhost\:2181			Comma-separated list of all ZooKeeper URLs in the destination cluster.
```

## Topology Parameters
```sh			
Property					Type (format)	Example								Comment
serializerEncodingValue		String			kafka.serializer.DefaultEncoder		For bytes, use the default kafka.serializer.DefaultEncoder. For string, use kafka.serializer.StringEncoder
partitionFieldName			String			bytes								Use bytes for bytes, and str for StringScheme.
schemeType					String			raw	Use raw for RawScheme (bytes), and string for StringScheme.
topologyName				String			datapipe-kafka
streamName					String			datapipe
requiredAcks				Integer			-1	
parallelCount				Integer			1	 
metricsParallelCount		Integer			1	 
topology.workers			Integer			1										Maximum value for this parameter is equal to the number of Storm supervisor nodes on the cluster.
```

Note : Only one source point at a time, either Kafka or RabbitMq



Author: [Narendra Bidari](https://github.com/supermonk)


# Contributions
Prior to receiving information from any contributor, Symantec requires that all contributors complete, sign, and submit Symantec Personal Contributor Agreement (SPCA). The purpose of the SPCA is to clearly define the terms under which intellectual property has been contributed to the project and thereby allow Symantec to defend the project should there be a legal dispute regarding the software at some future time. A signed SPCA is required to be on file before an individual is given commit privileges to the Symantec open source project. Please note that the privilege to commit to the project is conditional and may be revoked by Symantec.

If you are employed by a corporation, a Symantec Corporate Contributor Agreement (SCCA) is also required before you may contribute to the project. If you are employed by a company, you may have signed an employment agreement that assigns intellectual property ownership in certain of your ideas or code to your company. We require a SCCA to make sure that the intellectual property in your contribution is clearly contributed to the Symantec open source project, even if that intellectual property had previously been assigned by you.

Please complete the SPCA and, if required, the SCCA and return to Symantec at:

Symantec Corporation Legal Department Attention: Product Legal Support Team 350 Ellis Street Mountain View, CA 94043

Please be sure to keep a signed copy for your records.

# License
Copyright 2016 Symantec Corporation.

Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.

You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
