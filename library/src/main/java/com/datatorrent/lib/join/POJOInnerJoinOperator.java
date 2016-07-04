package com.datatorrent.lib.join;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ClassUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.PojoUtils;

public class POJOInnerJoinOperator extends AbstractInnerJoinOperator<Object,Object> implements Operator.ActivationListener<Context>
{
  protected Class<?> outputClass;
  private String stream1Class;
  private String stream2Class;
  private transient FieldObjectMap[] inputFieldObjects = (FieldObjectMap[])Array.newInstance(FieldObjectMap.class, 2);

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      if (context.getValue(Context.PortContext.TUPLE_CLASS) != null) {
        outputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
      }
    }
  };

  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> input1 = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      if (context.getValue(Context.PortContext.TUPLE_CLASS) != null) {
        inputFieldObjects[0].inputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
      }
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple,true);
    }
  };

  @InputPortFieldAnnotation(schemaRequired = true)
  public transient DefaultInputPort<Object> input2 = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      if (context.getValue(Context.PortContext.TUPLE_CLASS) != null) {
        inputFieldObjects[1].inputClass = context.getValue(Context.PortContext.TUPLE_CLASS);
      }
    }

    @Override
    public void process(Object tuple)
    {
      processTuple(tuple,false);
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    for (int i = 0; i < 2; i++) {
      inputFieldObjects[i] = new FieldObjectMap();
    }

    try {
      if (stream1Class != null) {
        inputFieldObjects[0].inputClass = this.getClass().getClassLoader().loadClass(stream1Class);
      }
      if (stream2Class != null) {
        inputFieldObjects[1].inputClass = this.getClass().getClassLoader().loadClass(stream2Class);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected void generateSettersAndGetters()
  {
    for (int i = 0; i < 2; i++) {
      Class inputClass = inputFieldObjects[i].inputClass;
      try {
        Class c = ClassUtils.primitiveToWrapper(inputClass.getField(keyFields.get(i)).getType());
        inputFieldObjects[i].keyGet = PojoUtils.createGetter(inputClass, keyFields.get(i), c);
        if (timeFields != null && timeFields.size() != 0) {
          Class tc = ClassUtils.primitiveToWrapper(inputClass.getField(timeFields.get(i)).getType());
          inputFieldObjects[i].timeFieldGet = PojoUtils.createGetter(inputClass, timeFields.get(i), tc);
        }
        for (int j = 0; j < includeFields[i].length; i++) {
          Class ic = ClassUtils.primitiveToWrapper(inputClass.getField(includeFields[i][j]).getType());
          Class oc = ClassUtils.primitiveToWrapper(outputClass.getField(includeFields[i][j]).getType());
          inputFieldObjects[i].fieldMap.put(PojoUtils.createGetter(inputClass, includeFields[i][j], ic),
              PojoUtils.createSetter(outputClass, includeFields[i][j], oc));
        }
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Object extractKey(Object tuple, boolean isStream1Data)
  {
    return isStream1Data ? inputFieldObjects[0].keyGet.get(tuple) :
      inputFieldObjects[1].keyGet.get(tuple);
  }

  @Override
  public Object joinTuples(List<Object> tuples)
  {
    Object o;
    try {
      o = outputClass.newInstance();
      for (int i = 0; i < 2; i++) {
        for (Map.Entry<PojoUtils.Getter,PojoUtils.Setter> g: inputFieldObjects[i].fieldMap.entrySet()) {
          g.getValue().set(o, g.getKey().get(tuples.get(i)));
        }
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    return o;
  }

  @Override
  public void emitTuple(Object tuple)
  {
    outputPort.emit(tuple);
  }

  @Override
  public void activate(Context context)
  {
    generateSettersAndGetters();
  }

  @Override
  public void deactivate()
  {

  }

  public String getStream1Class()
  {
    return stream1Class;
  }

  public void setStream1Class(String stream1Class)
  {
    this.stream1Class = stream1Class;
  }

  public String getStream2Class()
  {
    return stream2Class;
  }

  public void setStream2Class(String stream2Class)
  {
    this.stream2Class = stream2Class;
  }

  private class FieldObjectMap
  {
    public Class<?> inputClass;
    public PojoUtils.Getter keyGet;
    public PojoUtils.Getter timeFieldGet;
    public Map<PojoUtils.Getter,PojoUtils.Setter> fieldMap;

    public FieldObjectMap()
    {
      fieldMap = new HashMap<>();
    }
  }
}
