package com.datatorrent.contrib.dataflow;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.common.collect.ImmutableList;

public class AggregatorWrapper<AI, AO> implements Aggregator<AI, AO> {

  private AO aa;
  private Combine.CombineFn<AI, ?, AO> combiner;

  public AggregatorWrapper(Combine.CombineFn<AI, ?, AO> combiner) {
    this.combiner = combiner;
    resetLocal();
  }

  @SuppressWarnings("unchecked")
  public void add(AI value) {
    this.aa = combiner.apply(ImmutableList.of((AI) aa, value));
  }

  public void resetLocal() {
    this.aa = combiner.apply(ImmutableList.<AI>of());
  }

  @Override
  public void addValue(AI value) {
    add(value);
  }

  @Override
  public String getName() {
    return "Aggregator :" + combiner.toString();
  }

  @Override
  public Combine.CombineFn<AI, ?, AO> getCombineFn() {
    return combiner;
  }
}
