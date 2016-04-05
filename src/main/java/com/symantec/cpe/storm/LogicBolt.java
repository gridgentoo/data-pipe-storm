package com.symantec.cpe.storm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogicBolt implements IRichBolt {

  private static final long serialVersionUID = 1L;
  public static final Logger LOG = LoggerFactory.getLogger(LogicBolt.class);
  private transient OutputCollector collector;
  public static final String STREAM_NAME = "logic";
  public static final String FIELD_NAME = "logic";

  @Override
  public void cleanup() {
    // TODO Auto-generated method stub

  }

  @Override
  public void execute(Tuple input) {
    // For Now just sending, Add any logic to this section and do acking.
    LOG.debug("Input " + input.toString());
    this.collector.emit(STREAM_NAME, input, new Values(input));
    this.collector.ack(input);
    LOG.debug("Output " + input.toString());

  }

  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1,
      OutputCollector arg2) {
    // TODO Auto-generated method stub

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(STREAM_NAME, new Fields(FIELD_NAME));

  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

}