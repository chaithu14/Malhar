package com.datatorrent.lib.join;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableComplexComponent;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeObjectSlice;

public abstract class AbstractManagedStateInnerJoinOperator extends AbstractInnerJoinOperator
{
  private int noOfBuckets;
  private long bucketSpanTime;
  private SpillableStateStore stream1State;
  private SpillableStateStore stream2State;

  @Override
  public void createStores()
  {
    component = new ManagedStateSpillableComplexComponent(new ManagedStateSpillableStateStore());
    stream1State = new ManagedStateSpillableStateStore();
    stream2State = new ManagedStateSpillableStateStore();
    ((ManagedStateSpillableStateStore)stream1State).setNumBuckets(noOfBuckets);
    ((ManagedStateSpillableStateStore)stream2State).setNumBuckets(noOfBuckets);
    ((ManagedStateSpillableStateStore)stream1State).getTimeBucketAssigner().setBucketSpan(Duration.millis(bucketSpanTime));
    ((ManagedStateSpillableStateStore)stream2State).getTimeBucketAssigner().setBucketSpan(Duration.millis(bucketSpanTime));
    stream1Data = new SpillableByteArrayListMultimapImpl(stream1State,"a".getBytes(),0, new SerdeObjectSlice(), new SerdeObjectSlice());
    stream2Data = new SpillableByteArrayListMultimapImpl(stream2State,"b".getBytes(),0, new SerdeObjectSlice(), new SerdeObjectSlice());
  }
}
