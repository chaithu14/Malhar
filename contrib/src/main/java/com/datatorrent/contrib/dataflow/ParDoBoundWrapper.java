package com.datatorrent.contrib.dataflow;

import java.io.IOException;
import java.util.Collection;

import org.joda.time.Instant;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import com.datatorrent.api.DefaultOutputPort;

public class ParDoBoundWrapper<IN,OUT> extends AbstractParDoWrapper<IN,OUT,OUT>
{
  @Override
  public void outputWithTimestampHelper(WindowedValue<IN> inElement, OUT output, Instant timestamp, DefaultOutputPort out)
  {
    checkTimestamp(inElement, timestamp);
    out.emit(makeWindowedValue(
      output,
      timestamp,
      inElement.getWindows(),
      inElement.getPane()));
  }

  @Override
  public <T> void sideOutputWithTimestampHelper(WindowedValue<IN> inElement, T output, Instant timestamp, TupleTag<T> tag, DefaultOutputPort out)
  {
    // ignore the side output, this can happen when a user does not register
    // side outputs but then outputs using a freshly created TupleTag.
    throw new RuntimeException("sideOutput() not not available in ParDo.Bound().");
  }

  @Override
  public WindowingInternals<IN, OUT> windowingInternalsHelper(final WindowedValue<IN> inElement, final DefaultOutputPort out)
  {
    return new WindowingInternals<IN, OUT>() {
      @Override
      public StateInternals stateInternals() {
        throw new NullPointerException("StateInternals are not available for ParDo.Bound().");
      }

      @Override
      public void outputWindowedValue(OUT output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
        out.emit(makeWindowedValue(output, timestamp, windows, pane));
      }

      @Override
      public TimerInternals timerInternals() {
        throw new NullPointerException("TimeInternals are not available for ParDo.Bound().");
      }

      @Override
      public Collection<? extends BoundedWindow> windows() {
        return inElement.getWindows();
      }

      @Override
      public PaneInfo pane() {
        return inElement.getPane();
      }

      @Override
      public <T> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException
      {
        throw new RuntimeException("writePCollectionViewData() not supported in Streaming mode.");
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow) {
        throw new RuntimeException("sideInput() not implemented.");
      }
    };
  }
}
