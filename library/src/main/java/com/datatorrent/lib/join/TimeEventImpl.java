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

import javax.annotation.Nonnull;

/**
 * Time event Implementation.
 */
public class TimeEventImpl implements TimeEvent, Comparable<TimeEventImpl>
{
  protected Object key;
  protected long time;
  protected Object tuple;
  protected boolean match;

  @SuppressWarnings("unused")
  public TimeEventImpl()
  {
  }

  public TimeEventImpl(Object key, long time, Object tuple)
  {
    this.key = key;
    this.time = time;
    this.tuple = tuple;
    this.match = false;
  }

  @Override
  public long getTime()
  {
    return time;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimeEventImpl)) {
      return false;
    }

    TimeEventImpl that = (TimeEventImpl)o;

    if (time != that.time) {
      return false;
    }
    if (key != null ? !key.equals(that.key) : that.key != null) {
      return false;
    }

    if (tuple != null ? !tuple.equals(that.tuple) : that.tuple != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + (int)(time ^ (time >>> 32));
    return result;
  }

  @Override
  public Object getEventKey()
  {
    return key;
  }

  @Override
  public int compareTo(@Nonnull TimeEventImpl dummyEvent)
  {
    if (key.equals(dummyEvent.key)) {
      return 0;
    }
    return -1;
  }

  public Object getValue()
  {
    return tuple;
  }

  @Override
  public String toString()
  {
    return "TimeEvent{" +
      "key=" + key +
      ", time=" + time +
      ", tuple=" + tuple +
      ", match=" + match +
      '}';
  }

  public boolean isMatch()
  {
    return match;
  }

  public void setMatch(boolean match)
  {
    this.match = match;
  }
}

