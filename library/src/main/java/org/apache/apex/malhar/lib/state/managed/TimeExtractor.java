package org.apache.apex.malhar.lib.state.managed;

import org.apache.apex.malhar.lib.state.spillable.WindowListener;

/**
 * Created by siyuan on 9/27/16.
 */
public interface TimeExtractor<T> extends WindowListener
{
  long getTime(T t);
}
