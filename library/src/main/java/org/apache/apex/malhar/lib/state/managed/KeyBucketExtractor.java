package org.apache.apex.malhar.lib.state.managed;

public interface KeyBucketExtractor<T>
{
  long getBucket(T t);
}
