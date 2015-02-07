package com.datatorrent.contrib.kinesis;

public interface KinesisEncoder
{
  /**
   * Transforms object into byte array. Derived class has to implement this method.
   * @param arg0 Object data to transform
   * @return byte array
   */
  abstract public byte[] toBytes(Object arg0);
}