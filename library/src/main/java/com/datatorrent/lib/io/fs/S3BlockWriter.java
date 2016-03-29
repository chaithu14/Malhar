package com.datatorrent.lib.io.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.io.block.AbstractBlockReader;
import com.datatorrent.netlet.util.Slice;

public class S3BlockWriter extends BlockWriter
{
  private static final Logger LOG = LoggerFactory.getLogger(S3BlockWriter.class);

  @Override
  protected byte[] getBytesForTuple(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    LOG.info("getBytesForTuple: {} -> {}", tuple.getFilePath(), tuple.getPartNo());
    return tuple.getRecord().buffer;
  }
}

