package com.datatorrent.contrib.dataflow;

import java.util.Map;

import org.joda.time.Instant;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.join.RawUnionValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Preconditions;

import com.datatorrent.api.DefaultOutputPort;

public class ParDoBoundMultiWrapper<IN,OUT> extends AbstractParDoWrapper<IN,OUT,RawUnionValue>
{
  private final TupleTag<?> mainTag;
  private final Map<TupleTag<?>, Integer> outputLabels;

  public ParDoBoundMultiWrapper(PipelineOptions options, WindowingStrategy<?, ?> windowingStrategy, DoFn<IN, OUT> doFn, TupleTag<?> mainTag, Map<TupleTag<?>, Integer> tagsToLabels) {
    super(options, windowingStrategy, doFn);
    this.mainTag = Preconditions.checkNotNull(mainTag);
    this.outputLabels = Preconditions.checkNotNull(tagsToLabels);
  }

  @Override
  public void outputWithTimestampHelper(WindowedValue<IN> inElement, OUT output, Instant timestamp, DefaultOutputPort out)
  {
    checkTimestamp(inElement, timestamp);
    Integer index = outputLabels.get(mainTag);
    out.emit(makeWindowedValue(
      new RawUnionValue(index, output),
      timestamp,
      inElement.getWindows(),
      inElement.getPane()));
  }

  @Override
  public <T> void sideOutputWithTimestampHelper(WindowedValue<IN> inElement, T output, Instant timestamp, TupleTag<T> tag, DefaultOutputPort out)
  {
    checkTimestamp(inElement, timestamp);
    Integer index = outputLabels.get(tag);
    if (index != null) {
      out.emit(makeWindowedValue(
        new RawUnionValue(index, output),
        timestamp,
        inElement.getWindows(),
        inElement.getPane()));
    }
  }

  @Override
  public WindowingInternals<IN, OUT> windowingInternalsHelper(WindowedValue<IN> inElement, DefaultOutputPort out)
  {
    throw new RuntimeException("FlinkParDoBoundMultiWrapper is just an internal operator serving as " +
      "an intermediate transformation for the ParDo.BoundMulti translation. windowingInternals() " +
      "is not available in this class.");
  }
}
