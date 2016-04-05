package com.symantec.cpe.util;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

/**
 * Run the Topology Remotely or locally
 *
 */
public final class StormRunUtil {

  /** The Constant LOG. */
  private static final Logger LOG = Logger.getLogger(StormRunUtil.class);

  private StormRunUtil() {}

  public static LocalCluster runTopologyLocally(StormTopology topology, String topologyName,
      Config conf) {
    LocalCluster cluster = new LocalCluster();
    LOG.info("Started Running Topology Locally :" + topologyName);
    cluster.submitTopology(topologyName, conf, topology);
    LOG.info(" Topology submitted :" + topologyName);
    return cluster;
  }

  public static void runTopologyRemotely(StormTopology topology, String topologyName,
      Config conf) {
    LOG.info("Remote Toplogy Submission Started :" + topologyName);
    try {
      StormSubmitter.submitTopology(topologyName, conf, topology);
    } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
      LOG.error("Error while Running toplogy Remotely: ", e);
    }
    LOG.info("Finished Remote Topology Submission :" + topologyName);
  }
}