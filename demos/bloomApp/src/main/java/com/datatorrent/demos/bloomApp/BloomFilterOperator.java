/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.bloomApp;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
//import com.datatorrent.api.DAGContext;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Collection;

import java.io.*;

/*
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
*/
//public class BloomFilterOperator<T> extends BaseOperator implements Externalizable, Serializable
//@DefaultSerializer(ExternalizableSerializer.class)
public class BloomFilterOperator<T> extends BaseOperator implements Serializable
{

  private int expectedNumberOfElements;
  //private double falsePositiveProbability;
  static final Charset charset = Charset.forName("UTF-8"); // encoding used for storing hash values as strings
  private BloomFilter<T> bf = null;



  /*static {
    if(bf == null)
      bf = BloomFilter.create(strFunnel , expectedNumberOfElements, falsePositiveProbability);
  }*/
  /**
   * Constructs an empty Bloom filter. The total length of the Bloom filter will be
   * c*n.
   *
   * @param c is the number of bits used per element.
   * @param n is the expected number of elements the filter will contain.
   * @param k is the number of hash functions used.
   */

  transient Funnel<T> strFunnel = new Funnel<T>() {
    @Override
    public void funnel(T arg0, PrimitiveSink into) {
      //into.putString(arg0, Charsets.UTF_8);
      into.putBytes(arg0.toString().getBytes(charset));
    }
  };

 /* BloomFilterOperator()
  {
    if(this.bf == null)
      this.bf = BloomFilter.create(strFunnel , expectedNumberOfElements, falsePositiveProbability);

  }
  */

/*
  @SuppressWarnings("unchecked")
  public void writeExternal(ObjectOutput out) throws IOException
  {
    out.writeInt(this.expectedNumberOfElements);
    out.writeDouble(this.falsePositiveProbability);
    //out.writeObject(this.bf);
    bf.writeTo((ObjectOutputStream)out);
  }

  @SuppressWarnings("unchecked")
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
  {
    expectedNumberOfElements = in.readInt();
    falsePositiveProbability = in.readDouble();
    bf = BloomFilter.readFrom((ObjectInputStream)in, strFunnel);
  }
*/

  @SuppressWarnings("unchecked")
  public void writeExternal(OutputStream out) throws IOException
  {
    //out.write(this.expectedNumberOfElements);
    //byte[] bytes;
    //ByteBuffer.wrap(bytes).putDouble(this.falsePositiveProbability);
    //out.write(bytes);
    //out.writeObject(this.bf);
    bf.writeTo((ObjectOutputStream)out);
  }

  @SuppressWarnings("unchecked")
  public void readExternal(InputStream in) throws IOException, ClassNotFoundException
  {
    //expectedNumberOfElements = in.read();
    //falsePositiveProbability = in.read();
    /*Funnel<T> str1Funnel = new Funnel<T>() {
      @Override
      public void funnel(T arg0, PrimitiveSink into) {
        //into.putString(arg0, Charsets.UTF_8);
        into.putBytes(arg0.toString().getBytes(charset));
      }
    };
    strFunnel = str1Funnel;*/
    bf = BloomFilter.readFrom(in, strFunnel);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if(this.bf == null)
      this.bf = BloomFilter.create(strFunnel , expectedNumberOfElements, 0.01);
      //this.bf = BloomFilter.create(strFunnel , expectedNumberOfElements, falsePositiveProbability);
  }

  /**
   * Input port
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<T> data = new DefaultInputPort<T>()
  {
    /**
     * Adds the tuple into the BloomFilter
     */
    @Override
    public void process(T tuple)
    {
      if(!contains(tuple))
        unique.emit(tuple);
      add(tuple);
    }
  };


  /**
   * Output port
   */
  @OutputPortFieldAnnotation(name = "unique")
  public final transient DefaultOutputPort<T> unique = new DefaultOutputPort<T>();


  /**
   * End window operator override.
   */
  @Override
  public void endWindow()
  {
  }

  void add(T tuple)
  {
    bf.put(tuple);
  }

  boolean contains(T tuple)
  {
    return bf.mightContain(tuple);
  }

  public void setExpectedNumberOfElements(int expectedNumberOfElements)
  {
    this.expectedNumberOfElements = expectedNumberOfElements;
  }

  /*public void setFalsePositiveProbability(double falsePositiveProbability)
  {
    this.falsePositiveProbability = falsePositiveProbability;
  }*/

}
