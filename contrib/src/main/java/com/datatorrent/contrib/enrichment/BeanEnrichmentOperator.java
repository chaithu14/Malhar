package com.datatorrent.contrib.enrichment;


import com.datatorrent.api.Context;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class BeanEnrichmentOperator extends AbstractEnrichmentOperator<Object, Object> {

  public Class inputClass;
  public Class outputClass;
  private transient List<Field> fields = new ArrayList<Field>();
  private transient List<Field> updates = new ArrayList<Field>();

  @Override
  protected Object getKey(Object tuple) {
    ArrayList<Object> keyList = new ArrayList<Object>();
    for(Field f: fields) {
      try {
        keyList.add(f.get(tuple));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return keyList;
  }

  @Override
  protected Object convert(Object in, Object cached) {
    try {
      Object o = outputClass.newInstance();
      // Copy the fields from input to output
      Field[] fields = inputClass.getFields();
      for(Field f : fields) {
        f.set(o, f.get(in));
      }
      if (cached == null)
        return o;

      if(updates.size() == 0 && includeFields.size() != 0) {
        populateUpdatesFrmIncludeFields();
      }
      ArrayList<Object> newAttributes = (ArrayList<Object>)cached;
      int idx = 0;
      for(Field f : updates) {
        f.set(o, newAttributes.get(idx));
        idx++;
      }
      return o;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setup(Context.OperatorContext context) {
    super.setup(context);
    populateFieldsFrmLookup();
    populateUpdatesFrmIncludeFields();
  }

  private void populateFieldsFrmLookup() {
    for (String fName : lookupFields) {
      try {
        Field f = inputClass.getField(fName);
        f.setAccessible(true);
        fields.add(f);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void populateUpdatesFrmIncludeFields() {
    for (String fName : includeFields) {
      try {
        Field f = outputClass.getField(fName);
        updates.add(f);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
