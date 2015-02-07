package com.datatorrent.contrib.kinesis;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisJsonEncoder implements KinesisEncoder
{
  private static final Logger LOG = LoggerFactory.getLogger(KinesisJsonEncoder.class);
  private final ObjectMapper mapper = new ObjectMapper();

  KinesisJsonEncoder() {}

  @Override
  public byte[] toBytes(Object arg0)
  {
    try {
      return mapper.writeValueAsBytes(arg0);
    } catch (Exception e) {
      LOG.error("Failed to encode {}", arg0);
      throw new RuntimeException(e);
    }
  }
}