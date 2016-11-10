/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.state.spillable.managed;

import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;

import com.datatorrent.netlet.util.Slice;

/**
 *
 */
public class ManagedTimeUnifiedStateSpillableStateStore extends ManagedTimeUnifiedStateImpl implements SpillableStateStore
{
  @Override
  public void put(long bucketId, long time, @NotNull Slice key, @NotNull Slice value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Slice getSync(long bucketId, long time, @NotNull Slice key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<Slice> getAsync(long bucketId, long time, @NotNull Slice key)
  {
    throw new UnsupportedOperationException();
  }
}
