package com.symantec.cpe;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.symantec.cpe.config.ConfigHelper;
import com.symantec.cpe.storm.DataPipeTopology;

import backtype.storm.Config;

/**
 * Main Class to start the Reading and Writing This program reads the data from the Kafka End point
 * via Zookeeper setting and writes it back to new Kafka End point using Trident topology
 */
public class StartService {
  private static final Logger LOG = Logger.getLogger(StartService.class);

  public static Properties getLoadedProperties(String fileName) {
    Properties prop = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream(fileName);
      // load a properties file
      prop.load(input);
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return prop;
  }

  public static void main(String[] args) {
    // Read Property
    // <=1 error

    if (args == null || args.length < 1) {
      LOG.error("Input Validation failed, Below is the signature ");
      LOG.error(
          "USAGE : storm Jar " + StartService.class.getName() + "  <configuration file path>");
      return;
    }

    // 0, important to read args.length-1 because of bdse
    String inputFile = args[args.length-1];
    LOG.info("File Path \t " + inputFile);


    /* Get configurations */
    Config inputPropertyConf = ConfigHelper.loadConfig(inputFile);

    if (inputPropertyConf == null) {
      LOG.error("Error in the input Property Configuration");
      return;
    }
    // Run
    DataPipeTopology.buildToplogyAndSubmit(inputPropertyConf);

  }

}
