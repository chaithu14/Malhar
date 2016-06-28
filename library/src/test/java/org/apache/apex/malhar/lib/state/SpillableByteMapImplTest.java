package org.apache.apex.malhar.lib.state;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.FSWindowDataManagerTest;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.util.TestUtils;

/**
 * Created by tfarkas on 6/6/16.
 */
public class SpillableByteMapImplTest
{
  public static final byte[] ID1 = new byte[]{(byte)0};
  public static final byte[] ID2 = new byte[]{(byte)1};

  private static class TestMeta extends TestWatcher
  {

    String applicationPath;
    Context.OperatorContext context;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      super.starting(description);
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();

      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_PATH, applicationPath);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
    }

    @Override
    protected void finished(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  @Rule
  public SpillableByteMapImplTest.TestMeta testMeta = new SpillableByteMapImplTest.TestMeta();

  @Test
  public void simpleGetAndPutTest()
  {
    SerdeStringSlice sss = new SerdeStringSlice();

    ManagedStateSpillableStateStore store = new ManagedStateSpillableStateStore();
    ((FileAccessFSImpl)store.getFileAccess()).setBasePath(testMeta.applicationPath);
    store.getTimeBucketAssigner().setBucketSpan(Duration.millis(5000));
    //InMemSpillableStateStore store = new InMemSpillableStateStore();
    SpillableByteMapImpl<String, String> map = new SpillableByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice());

    store.setup(testMeta.context);
    map.setup(testMeta.context);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    long windowId = 0L;
    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(0, map.size());

    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");

    Assert.assertEquals(3, map.size());

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals("2", map.get("b"));
    Assert.assertEquals("3", map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(3, map.size());

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals("2", map.get("b"));
    Assert.assertEquals("3", map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(6, map.size());

    Assert.assertEquals("4", map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, null);

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, "4");
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);
  }

  @Test
  public void simpleMultiGetAndPutTest()
  {
    SerdeStringSlice sss = new SerdeStringSlice();

    ManagedStateSpillableStateStore store = new ManagedStateSpillableStateStore();
    ((FileAccessFSImpl)store.getFileAccess()).setBasePath(testMeta.applicationPath);
    store.getTimeBucketAssigner().setBucketSpan(Duration.millis(5000));
    //InMemSpillableStateStore store = new InMemSpillableStateStore();
    SpillableByteArrayListMultimapImpl<String, String> map = new SpillableByteArrayListMultimapImpl<>(store, ID1, 0L,
      new SerdeStringSlice(),
      new SerdeStringSlice());

    store.setup(testMeta.context);
    map.setup(testMeta.context);
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    long windowId = 0L;
    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(0, map.size());

    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");
    map.endWindow();
    Assert.assertEquals(3, map.size());

    Assert.assertEquals("1", map.get("a").get(0));
    Assert.assertEquals("2", map.get("b").get(0));
    Assert.assertEquals("3", map.get("c").get(0));
    Assert.assertEquals(null, map.get("d"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(3, map.size());

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals("2", map.get("b"));
    Assert.assertEquals("3", map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(6, map.size());

    Assert.assertEquals("4", map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, null);

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, "3");
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, "4");
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);
  }

  @Test
  public void simpleRemoveTest()
  {
    SerdeStringSlice sss = new SerdeStringSlice();

    InMemSpillableStateStore store = new InMemSpillableStateStore();
    SpillableByteMapImpl<String, String> map = new SpillableByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice());

    long windowId = 0L;
    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(0, map.size());

    map.put("a", "1");
    map.put("b", "2");
    map.put("c", "3");

    Assert.assertEquals(3, map.size());

    map.remove("b");
    map.remove("c");

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    Assert.assertEquals(1, map.size());

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);

    map.beginWindow(windowId);
    windowId++;

    Assert.assertEquals(1, map.size());

    Assert.assertEquals("1", map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));

    map.put("d", "4");
    map.put("e", "5");
    map.put("f", "6");

    Assert.assertEquals(4, map.size());

    Assert.assertEquals("4", map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, null);

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, "4");
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);

    map.beginWindow(windowId);
    windowId++;

    map.remove("a");
    map.remove("d");
    Assert.assertEquals(null, map.get("a"));
    Assert.assertEquals(null, map.get("b"));
    Assert.assertEquals(null, map.get("c"));
    Assert.assertEquals(null, map.get("d"));
    Assert.assertEquals("5", map.get("e"));
    Assert.assertEquals("6", map.get("f"));
    Assert.assertEquals(null, map.get("g"));

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, "4");
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);

    map.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "d", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "e", ID1, "5");
    SpillableTestUtils.checkValue(store, 0L, "f", ID1, "6");
    SpillableTestUtils.checkValue(store, 0L, "g", ID1, null);
  }

  @Test
  public void multiMapPerBucketTest()
  {
    SerdeStringSlice sss = new SerdeStringSlice();

    InMemSpillableStateStore store = new InMemSpillableStateStore();
    SpillableByteMapImpl<String, String> map1 = new SpillableByteMapImpl<>(store, ID1, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice());
    SpillableByteMapImpl<String, String> map2 = new SpillableByteMapImpl<>(store, ID2, 0L,
        new SerdeStringSlice(),
        new SerdeStringSlice());

    long windowId = 0L;
    map1.beginWindow(windowId);
    map2.beginWindow(windowId);
    windowId++;

    map1.put("a", "1");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals(null, map2.get("a"));

    map2.put("a", "a1");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals("a1", map2.get("a"));

    map1.put("b", "2");
    map2.put("c", "3");

    Assert.assertEquals("1", map1.get("a"));
    Assert.assertEquals("2", map1.get("b"));

    Assert.assertEquals("a1", map2.get("a"));
    Assert.assertEquals(null, map2.get("b"));
    Assert.assertEquals("3", map2.get("c"));

    map1.endWindow();
    map2.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, "1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID1, "2");

    SpillableTestUtils.checkValue(store, 0L, "a", ID2, "a1");
    SpillableTestUtils.checkValue(store, 0L, "b", ID2, null);
    SpillableTestUtils.checkValue(store, 0L, "c", ID2, "3");

    map1.beginWindow(windowId);
    map2.beginWindow(windowId);
    windowId++;

    map1.remove("a");

    Assert.assertEquals(null, map1.get("a"));
    Assert.assertEquals("a1", map2.get("a"));

    map1.endWindow();
    map2.endWindow();

    SpillableTestUtils.checkValue(store, 0L, "a", ID1, null);
    SpillableTestUtils.checkValue(store, 0L, "a", ID2, "a1");
  }
}
