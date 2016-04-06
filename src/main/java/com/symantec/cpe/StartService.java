/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
