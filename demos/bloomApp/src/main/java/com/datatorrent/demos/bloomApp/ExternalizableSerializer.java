package com.datatorrent.demos.bloomApp;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Collection;

public class ExternalizableSerializer<T extends Externalizable> extends Serializer<T>
{
  @Override
  public void write(Kryo kryo, Output output, T object)
  {
    try {
      ObjectOutputStream stream;
      object.writeExternal(stream = new ObjectOutputStream(output));
      stream.flush();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public T read(Kryo kryo, Input input, Class<T> type)
  {
    T object = kryo.newInstance(type);
    kryo.reference(object);
    try {
      object.readExternal(new ObjectInputStream(input));
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return object;
  }

}

class BloomFilter2Operator<T> extends BaseOperator
{
  private BitSet bitset;
  private int bitSetSize;
  private double bitsPerElement;
  private int expectedNumberOfFilterElements; // expected (maximum) number of elements to be added
  private int numberOfAddedElements; // number of elements actually added to the Bloom filter
  private int expectedNumberOfElements;
  private int k; // number of hash functions
  private double falsePositiveProbability;

  static final Charset charset = Charset.forName("UTF-8"); // encoding used for storing hash values as strings

  static final String hashName = "MD5"; // MD5 gives good enough accuracy in most circumstances. Change to SHA1 if it's needed
  static final MessageDigest digestFunction;
  static { // The digest method is reused between instances
    MessageDigest tmp;
    try {
      tmp = java.security.MessageDigest.getInstance(hashName);
    } catch (NoSuchAlgorithmException e) {
      tmp = null;
    }
    digestFunction = tmp;
  }
  /**
   * Constructs an empty Bloom filter. The total length of the Bloom filter will be
   * c*n.
   *
   * @param c is the number of bits used per element.
   * @param n is the expected number of elements the filter will contain.
   * @param k is the number of hash functions used.
   */
  public void SetAttributes(double c, int n, int k) {
    this.expectedNumberOfFilterElements = n;
    this.k = k;
    this.bitsPerElement = c;
    this.bitSetSize = (int)Math.ceil(c * n);
    numberOfAddedElements = 0;
    if(this.bitset == null) {
      System.out.println("Creating new bit set: ");
      this.bitset = new BitSet(bitSetSize);
    }
    System.out.println("----------------No of exp elements: " + n);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    SetAttributes(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2), // c = k / ln(2)
        expectedNumberOfElements,
        (int)Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))));
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
      processTuple(tuple);
    }
  };

  public void processTuple(T tuple)
  {
    if(!contains(tuple))
      unique.emit(tuple);

    add(tuple);
  }

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

  /**
   * Generates a digest based on the contents of a String.
   *
   * @param val specifies the input data.
   * @param charset specifies the encoding of the input data.
   * @return digest as long.
   */
  public static int createHash(String val, Charset charset) {
    return createHash(val.getBytes(charset));
  }

  /**
   * Generates a digest based on the contents of a String.
   *
   * @param val specifies the input data. The encoding is expected to be UTF-8.
   * @return digest as long.
   */
  public static int createHash(String val) {
    return createHash(val, charset);
  }

  /**
   * Generates a digest based on the contents of an array of bytes.
   *
   * @param data specifies input data.
   * @return digest as long.
   */
  public static int createHash(byte[] data) {
    return createHashes(data, 1)[0];
  }

  /**
   * Generates digests based on the contents of an array of bytes and splits the result into 4-byte int's and store them in an array. The
   * digest function is called until the required number of int's are produced. For each call to digest a salt
   * is prepended to the data. The salt is increased by 1 for each call.
   *
   * @param data specifies input data.
   * @param hashes number of hashes/int's to produce.
   * @return array of int-sized hashes
   */
  public static int[] createHashes(byte[] data, int hashes) {
    int[] result = new int[hashes];

    int k = 0;
    byte salt = 0;
    while (k < hashes) {
      byte[] digest;
      //synchronized (digestFunction) {
      digestFunction.update(salt);
      salt++;
      digest = digestFunction.digest(data);
      //}
      //System.out.println("Digest Length: " + digest.length);
      //for (byte b : digest) {
      //    System.out.println(Integer.toBinaryString(b & 255 | 256).substring(1));
      //}
      for (int i = 0; i < digest.length/4 && k < hashes; i++) {
        int h = 0;
        for (int j = (i*4); j < (i*4)+4; j++) {
          h <<= 8;
          h |= ((int) digest[j]) & 0xFF;
        }
        //System.out.println("Hash value at " + k + " : " + h);
        result[k] = h;
        k++;
      }
    }
    return result;
  }

  /**
   * Calculates the expected probability of false positives based on
   * the number of expected filter elements and the size of the Bloom filter.
   * <br /><br />
   * The value returned by this method is the <i>expected</i> rate of false
   * positives, assuming the number of inserted elements equals the number of
   * expected elements. If the number of elements in the Bloom filter is less
   * than the expected value, the true probability of false positives will be lower.
   *
   * @return expected probability of false positives.
   */
  public double expectedFalsePositiveProbability() {
    return getFalsePositiveProbability(expectedNumberOfFilterElements);
  }

  /**
   * Calculate the probability of a false positive given the specified
   * number of inserted elements.
   *
   * @param numberOfElements number of inserted elements.
   * @return probability of a false positive.
   */
  public double getFalsePositiveProbability(double numberOfElements) {
    // (1 - e^(-k * n / m)) ^ k
    return Math.pow((1 - Math.exp(-k * (double) numberOfElements
        / (double) bitSetSize)), k);

  }

  /**
   * Get the current probability of a false positive. The probability is calculated from
   * the size of the Bloom filter and the current number of elements added to it.
   *
   * @return probability of false positives.
   */
  public double getFalsePositiveProbability() {
    return getFalsePositiveProbability(numberOfAddedElements);
  }


  /**
   * Returns the value chosen for K.<br />
   * <br />
   * K is the optimal number of hash functions based on the size
   * of the Bloom filter and the expected number of inserted elements.
   *
   * @return optimal k.
   */
  public int getK() {
    return k;
  }

  /**
   * Sets all bits to false in the Bloom filter.
   */
  public void clear() {
    bitset.clear();
    numberOfAddedElements = 0;
  }

  /**
   * Adds an object to the Bloom filter. The output from the object's
   * toString() method is used as input to the hash functions.
   *
   * @param tuple is an element to register in the Bloom filter.
   */
  public void add(T tuple) {
    add(tuple.toString().getBytes(charset));
  }

  /**
   * Adds an array of bytes to the Bloom filter.
   *
   * @param bytes array of bytes to add to the Bloom filter.
   */
  public void add(byte[] bytes) {
    int[] hashes = createHashes(bytes, k);
    for (int hash : hashes)
      bitset.set(Math.abs(hash % bitSetSize), true);
    numberOfAddedElements ++;
  }

  /**
   * Returns true if the element could have been inserted into the Bloom filter.
   * Use getFalsePositiveProbability() to calculate the probability of this
   * being correct.
   *
   * @param element element to check.
   * @return true if the element could have been inserted into the Bloom filter.
   */
  public boolean contains(T element) {
    return contains(element.toString().getBytes(charset));
  }

  /**
   * Returns true if the array of bytes could have been inserted into the Bloom filter.
   * Use getFalsePositiveProbability() to calculate the probability of this
   * being correct.
   *
   * @param bytes array of bytes to check.
   * @return true if the array could have been inserted into the Bloom filter.
   */
  public boolean contains(byte[] bytes) {
    int[] hashes = createHashes(bytes, k);
    for (int hash : hashes) {
      if (!bitset.get(Math.abs(hash % bitSetSize))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if all the elements of a Collection could have been inserted
   * into the Bloom filter. Use getFalsePositiveProbability() to calculate the
   * probability of this being correct.
   * @param c elements to check.
   * @return true if all the elements in c could have been inserted into the Bloom filter.
   */
  public boolean containsAll(Collection<? extends T> c) {
    for (T element : c)
      if (!contains(element))
        return false;
    return true;
  }

  /**
   * Read a single bit from the Bloom filter.
   * @param bit the bit to read.
   * @return true if the bit is set, false if it is not.
   */
  public boolean getBit(int bit) {
    return bitset.get(bit);
  }

  /**
   * Set a single bit in the Bloom filter.
   * @param bit is the bit to set.
   * @param value If true, the bit is set. If false, the bit is cleared.
   */
  public void setBit(int bit, boolean value) {
    bitset.set(bit, value);
  }

  /**
   * Return the bit set used to store the Bloom filter.
   * @return bit set representing the Bloom filter.
   */
  public BitSet getBitSet() {
    return bitset;
  }

  /**
   * Returns the number of bits in the Bloom filter. Use count() to retrieve
   * the number of inserted elements.
   *
   * @return the size of the bitset used by the Bloom filter.
   */
  public int size() {
    return this.bitSetSize;
  }

  /**
   * Returns the number of elements added to the Bloom filter after it
   * was constructed or after clear() was called.
   *
   * @return number of elements added to the Bloom filter.
   */
  public int count() {
    return this.numberOfAddedElements;
  }

  /**
   * Returns the expected number of elements to be inserted into the filter.
   * This value is the same value as the one passed to the constructor.
   *
   * @return expected number of elements.
   */
  public int getExpectedNumberOfElements() {
    return expectedNumberOfFilterElements;
  }

  /**
   * Get expected number of bits per element when the Bloom filter is full. This value is set by the constructor
   * when the Bloom filter is created. See also getBitsPerElement().
   *
   * @return expected number of bits per element.
   */
  public double getExpectedBitsPerElement() {
    return this.bitsPerElement;
  }

  /**
   * Get actual number of bits per element based on the number of elements that have currently been inserted and the length
   * of the Bloom filter. See also getExpectedBitsPerElement().
   *
   * @return number of bits per element.
   */
  public double getBitsPerElement() {
    return this.bitSetSize / (double)numberOfAddedElements;
  }

  public void setExpectedNumberOfElements(int expectedNumberOfElements)
  {
    this.expectedNumberOfElements = expectedNumberOfElements;
  }

  public void setFalsePositiveProbability(double falsePositiveProbability)
  {
    this.falsePositiveProbability = falsePositiveProbability;
  }
}
