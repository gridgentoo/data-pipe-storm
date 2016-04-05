package com.symantec.cpe.storm;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.symantec.cpe.config.Constants;
import com.symantec.cpe.config.DO.RabbitConnectionConfigDO;

import backtype.storm.Config;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import io.latent.storm.rabbitmq.RabbitMQSpout;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class StreamBuilder {
  private static final Logger LOG = Logger.getLogger(StreamBuilder.class);


  /**
   * 
   * @param topology
   * @param inputPropertyConf
   * @param producerConf
   * @return
   */
  private static IRichSpout getKafkaSpout(String spoutName, TopologyBuilder builder,
      Config inputPropertyConf, int spoutParallelHint) {

    String streamName = null;
    String inputZookeeperURL = null;
    String inputTopicNameKafka = null;
    String schemeType = null;

    try {
      if (inputPropertyConf == null || inputPropertyConf.isEmpty()) {
        LOG.error("Error is loading property file" + inputPropertyConf);
        return null;
      }

      streamName = inputPropertyConf.get(Constants.STREAM_NAME_STRING).toString();
      inputZookeeperURL = inputPropertyConf.get(Constants.SOURCE_ZOOKEEPER_URL_STRING).toString();;
      inputTopicNameKafka = inputPropertyConf.get(Constants.INPUT_TOPIC_STRING).toString();
      schemeType = inputPropertyConf.get(Constants.SCHEME_TYPE_STRING).toString();

    } catch (Exception e) {
      LOG.error("Error in processing property file" + e.getMessage());
    }
    Scheme scheme = SchemeBuilder.getScheme(schemeType);
    IRichSpout spout = getKafkaSpout(scheme, inputZookeeperURL, inputTopicNameKafka, streamName);
    builder.setSpout(spoutName, spout).setMaxTaskParallelism(spoutParallelHint);
    return spout;
  }



  /**
   * Builds the OpaqueTridentKafkaSpout via Zookeeper and Topic Name
   * 
   * @param scheme
   * @param zookeeperURL
   * @param topicName
   * @return
   */
  private static IRichSpout getKafkaSpout(Scheme scheme, String zookeeperURL, String topicName,
      String groupId) {
    BrokerHosts hosts = new ZkHosts(zookeeperURL);

    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, groupId);
    
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
    return kafkaSpout;
  }



  private static RabbitMQSpout getRabbitMqSpout(String spoutName, TopologyBuilder builder,
      Config inputPropertyConf, int spoutParallelHint) {

    String schemeType = null;
    // TODO
    int shardCount = 1;
    List<RabbitConnectionConfigDO> queueConfigList = new ArrayList<>();
    try {
      if (inputPropertyConf == null || inputPropertyConf.isEmpty()) {
        LOG.error("Error is loading property file" + inputPropertyConf);
        return null;
      }

      schemeType = inputPropertyConf.get(Constants.SCHEME_TYPE_STRING).toString();
      shardCount =
          Integer.parseInt(inputPropertyConf.get(Constants.RABBITMQ_SHARDCOUNT).toString());
      for (int i = 0; i < shardCount; i++) {
        RabbitConnectionConfigDO configDO = new RabbitConnectionConfigDO(i, inputPropertyConf);
        queueConfigList.add(configDO);
      }
    } catch (Exception e) {
      LOG.error("Error in processing property file" + e);
    }

    RabbitMQSpout spout = null;

    //TODO , try to read from multiple queues and merge to single stream.
    for (int i = 0; i < 1; i++) {
      Scheme scheme = SchemeBuilder.getScheme(schemeType);
      RabbitConnectionConfigDO configDO = queueConfigList.get(i);
      spout = getRabbitMqSpout(scheme, spoutName, configDO.getConfig());

      builder.setSpout(spoutName, spout).addConfigurations(configDO.getConfig())
          .setMaxTaskParallelism(spoutParallelHint);

    }

    return spout;

  }


  /**
   * 
   * @param scheme
   * @param producerConf
   * @return
   */
  private static RabbitMQSpout getRabbitMqSpout(Scheme scheme, String streamId,
      Config producerConf) {
    RabbitMQSpout spout = new RabbitMQSpout(scheme, producerConf);
    LOG.info("Finished Building RabbitMQSpout");
    return spout;
  }

  /**
   * Builds the Stream with required Spout
   * 
   * @return
   */
  public static IRichSpout setSpout(String spoutName, String spoutType, Config inputPropertyConf,
      TopologyBuilder builder, int spoutParallelHint) {

    if (spoutType == null || spoutType.length() == 0) {
      LOG.error("Input Spout Type is wrong");
      return null;
    }

    try {
      if (inputPropertyConf == null || inputPropertyConf.isEmpty()) {
        LOG.error("Error is loading property file" + inputPropertyConf);
        return null;
      }


    } catch (Exception e) {
      LOG.error("Error in processing property file" + e);
    }


    if (spoutType.toLowerCase().contains("rabbit")) {
      return getRabbitMqSpout(spoutName, builder, inputPropertyConf, spoutParallelHint);
    } else {
      return getKafkaSpout(spoutName, builder, inputPropertyConf, spoutParallelHint);
    }
  }



}
