package org.apache.apex.malhar.lib.state.spillable.managed;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;

import com.datatorrent.netlet.util.Slice;

public class ManagedTimeStateStore extends ManagedTimeStateImpl implements SpillableStateStore
{
  @Override
  public void put(long bucketId, @NotNull Slice key, @NotNull Slice value)
  {
  }
}
