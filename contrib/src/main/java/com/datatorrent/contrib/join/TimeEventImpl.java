package com.datatorrent.contrib.join;

import javax.annotation.Nonnull;

/**
 * Time event.
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

  public TimeEventImpl(Object key, long time,Object tuple)
  {
    this.key = key;
    this.time = time;
    this.tuple = tuple;
    this.match = false;
  }

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

    TimeEventImpl that = (TimeEventImpl) o;

    if (time != that.time) {
      return false;
    }
    if (key != null ? !key.equals(that.key) : that.key != null) {
      return false;
    }

    if (tuple != null ? ! tuple.equals(that.tuple) : that.tuple != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + (int) (time ^ (time >>> 32));
    return result;
  }

  public Object getEventKey()
  {
    return key;
  }

  @Override
  public int compareTo(@Nonnull TimeEventImpl dummyEvent)
  {
    if(key.equals(dummyEvent.key)) {
      return 0;
    }
    return -1;
  }

  public Object getValue()
  {
    return tuple;
  }

  @Override public String toString()
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

