package org.apache.apex.malhar.kafka;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaHelper implements Serializer<KafkaOutputOperatorTest.Person>, Deserializer<KafkaOutputOperatorTest.Person>
{
  @Override
  public KafkaOutputOperatorTest.Person deserialize(String s, byte[] bytes)
  {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int nameLength = byteBuffer.getInt();
    byte[] name = new byte[nameLength];

    byteBuffer.get(name, 0, nameLength);

    return new KafkaOutputOperatorTest.Person(new String(name), byteBuffer.getInt());
  }

  @Override
  public byte[] serialize(String s, KafkaOutputOperatorTest.Person person)
  {
    byte[] name = person.name.getBytes();

    ByteBuffer byteBuffer = ByteBuffer.allocate(name.length + 4 + 4);

    byteBuffer.putInt(name.length);
    byteBuffer.put(name);
    byteBuffer.putInt(person.age);

    return byteBuffer.array();
  }

  @Override
  public void configure(Map<String, ?> map, boolean b)
  {
  }

  @Override
  public void close()
  {
  }
}
