/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.kafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class EmbeddedKafka
{
  private static final String KAFKA_PATH = "/tmp/kafka-test";

  private ZkClient zkClient;
  private ZkUtils zkUtils;
  private String BROKERHOST = "localhost";
  private ZooKeeperServer[] zkServer = new ZooKeeperServer[2];
  private KafkaServer kafkaServer;
  public static final int[] TEST_ZOOKEEPER_PORT;
  public static final int[] TEST_KAFKA_BROKER_PORT;
  public static String baseDir = "target";

  private static final String zkBaseDir = "zookeeper-server-data";
  private static final String kafkaBaseDir = "kafka-server-data";
  private static final String[] zkdir = new String[]{"zookeeper-server-data1", "zookeeper-server-data2"};
  private static final String[] zklogdir = new String[]{"zookeeper-log-data1", "zookeeper-log-data2"};
  private static ServerCnxnFactory[] zkFactory = new ServerCnxnFactory[2];
  static final org.slf4j.Logger logger = LoggerFactory.getLogger(EmbeddedKafka.class);

  // get available ports
  static {
    ServerSocket[] listeners = new ServerSocket[4];
    int[] p = new int[4];

    try {
      for (int i = 0; i < 4; i++) {
        listeners[i] = new ServerSocket(0);
        p[i] = listeners[i].getLocalPort();
      }

      for (int i = 0; i < 4; i++) {
        listeners[i].close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    TEST_ZOOKEEPER_PORT = new int[]{p[0], p[1]};
    TEST_KAFKA_BROKER_PORT = new int[]{p[2], p[3]};
  }

  public void startZookeeper(int clusterId)
  {
    try {
      int numConnections = 100;
      int tickTime = 2000;
      File snapshotDir;
      File logDir;
      try {
        snapshotDir = java.nio.file.Files.createTempDirectory(zkdir[clusterId]).toFile();
        logDir = java.nio.file.Files.createTempDirectory(zklogdir[clusterId]).toFile();
      } catch (IOException e) {
        throw new RuntimeException("Unable to start Kafka", e);
      }

      snapshotDir.deleteOnExit();
      logDir.deleteOnExit();
      zkServer[clusterId] = new ZooKeeperServer(snapshotDir, logDir, tickTime);
      zkFactory[clusterId] = new NIOServerCnxnFactory();
      zkFactory[clusterId].configure(new InetSocketAddress(TEST_ZOOKEEPER_PORT[clusterId]), numConnections);

      zkFactory[clusterId].startup(zkServer[clusterId]); // start the zookeeper server.
      Thread.sleep(2000);
      //kserver.startup();
    } catch (Exception ex) {
      logger.error(ex.getLocalizedMessage());
    }
  }

  public void stopZookeeper(int clusterId)
  {
    zkServer[clusterId].shutdown();
    zkFactory[clusterId].closeAll();
    zkFactory[clusterId].shutdown();
  }

  public String getBroker()
  {
    return BROKERHOST + ":" + TEST_KAFKA_BROKER_PORT[0];
  }

  public void start() throws IOException
  {
    FileUtils.deleteDirectory(new File(KAFKA_PATH));
    // Setup Zookeeper
    //zkServer = new EmbeddedZookeeper();
    startZookeeper(0);
    startZookeeper(1);
    String zkConnect = BROKERHOST + ":" + TEST_ZOOKEEPER_PORT[0];
    zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    zkUtils = ZkUtils.apply(zkClient, false);

    // Setup brokers
    cleanupDir();
    Properties props = new Properties();
    props.setProperty("zookeeper.connect", zkConnect);
    props.setProperty("broker.id", "0");
    props.setProperty("log.dirs", KAFKA_PATH);
    props.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + TEST_KAFKA_BROKER_PORT[0]);
    KafkaConfig config = new KafkaConfig(props);
    kafkaServer = TestUtils.createServer(config, new SystemTime());
    //kafkaServer = TestUtils.createServer(config, mock);
  }

  public void stop() throws IOException
  {
    kafkaServer.shutdown();
    zkClient.close();
    stopZookeeper(0);
    stopZookeeper(1);
    cleanupDir();
  }

  private void cleanupDir() throws IOException
  {
    FileUtils.deleteDirectory(new File(KAFKA_PATH));
  }

  public void createTopic(String topic)
  {
    AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
    List<KafkaServer> servers = new ArrayList<KafkaServer>();
    servers.add(kafkaServer);
    //TestUtils.createTopic(zkUtils, topic, 1, 1, servers, new Properties());
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 30000);
  }

  public void publish(String topic, List<String> messages)
  {
    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + TEST_KAFKA_BROKER_PORT[0]);
    producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    try (KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(producerProps)) {
      for (String message : messages) {
        ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(topic, message.getBytes(StandardCharsets.UTF_8));
        producer.send(data);
      }
    }

    List<KafkaServer> servers = new ArrayList<KafkaServer>();
    servers.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 30000);
  }

  public List<String> consume(String topic, int timeout)
  {
    return consume(topic, timeout, true);
  }

  public List<String> consume(String topic, int timeout, boolean earliest)
  {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + TEST_KAFKA_BROKER_PORT[0]);
    consumerProps.setProperty("group.id", "group0");
    consumerProps.setProperty("client.id", "consumer0");
    consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
    consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    // to make sure the consumer starts from the beginning of the topic
    consumerProps.put("auto.offset.reset", earliest ? "earliest" : "latest");
    KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Arrays.asList(topic));

    List<String> messages = new ArrayList<>();

    ConsumerRecords<Integer, byte[]> records = consumer.poll(timeout);
    for (ConsumerRecord<Integer, byte[]> record : records) {
      messages.add(new String(record.value()));
    }

    consumer.close();

    return messages;
  }

}
