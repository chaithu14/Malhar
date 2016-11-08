package org.apache.apex.malhar.lib.state.spillable;

import org.apache.apex.malhar.lib.state.TimeSlicedBucketedState;
import org.apache.apex.malhar.lib.state.managed.BucketProvider;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

@InterfaceStability.Evolving
public interface SpillableTimeStateStore extends TimeSlicedBucketedState, Component<Context.OperatorContext>,
    Operator.CheckpointNotificationListener, WindowListener, BucketProvider
{

}
