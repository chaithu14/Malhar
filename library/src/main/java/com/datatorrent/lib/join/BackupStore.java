/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.join;

/**
 * <p>
 *  Interface of store for join operation.
 * </p>
 */
public interface BackupStore
{
  /**
   * Generate the store
   */
  void setup();

  void committed(long windowId);

  void checkpointed(long windowId);

  void endWindow();

  /**
   * Get the key from the given tuple and with that key, get the tuples which satisfies the join constraint from the store.
   * @param tuple
   * @return
   */
  Object getValidTuples(Object tuple);

  /**
   * Insert the given tuple
   * @param tuple
   */
  Boolean put(Object tuple);

  /**
   * Shutdown the services.
   */
  void shutdown();

  /**
   * Return the unmatched events from store
   * @return
   */
  Object getUnMatchedTuples();

  /**
   * Set if the join type is outer
   * @param isOuter
   */
  void isOuterJoin(Boolean isOuter);
}
