package com.symantec.cpe.storm.mapper;

import org.apache.log4j.Logger;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;

/**
 * Overriding the default TridentTupleToKafkaMapper as we are trying to build generic Mapper
 *
 */
@SuppressWarnings("serial")
public class KafkaTupleToKafkaMapper implements TupleToKafkaMapper<Object, Object> {

  private static final Logger LOG = Logger.getLogger(TupleToKafkaMapper.class);
  String fieldName = "str"; // Raw bytes for default

  public KafkaTupleToKafkaMapper(String fieldName) {
    this.fieldName = fieldName;
  }

 

  @Override
  public Object getKeyFromTuple(Tuple tuple) {    
    return null;
  }

  @Override
  public Object getMessageFromTuple(Tuple tuple) {
    try {
      return tuple.getValueByField(this.fieldName);
    } catch (Exception e) {
      LOG.error("Error while getting message from tuple \t" + this.fieldName, e);
    }
    return null;
  }

}
