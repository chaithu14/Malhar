package com.datatorrent.demos.dimensions.generic;

/**
 * A single sales event
 */
public class SalesEvent {

  /* dimension keys */
  public long timestamp;
  public int productId;
  public int customerId;
  public int channelId;
  public int regionId;
  public int productCategory;
  /* metrics */
  public double amount;
  public double discount;
  public double tax;
  public byte[] bytesData;

  public double getTax()
  {
    return tax;
  }

  public void setTax(double tax)
  {
    this.tax = tax;
  }

  public double getDiscount()
  {
    return discount;
  }

  public void setDiscount(double discount)
  {
    this.discount = discount;
  }

  public double getAmount()
  {
    return amount;
  }

  public void setAmount(double amount)
  {
    this.amount = amount;
  }

  public int getProductCategory()
  {
    return productCategory;
  }

  public void setProductCategory(int productCategory)
  {
    this.productCategory = productCategory;
  }

  public int getRegionId()
  {
    return regionId;
  }

  public void setRegionId(int regionId)
  {
    this.regionId = regionId;
  }

  public int getChannelId()
  {
    return channelId;
  }

  public void setChannelId(int channelId)
  {
    this.channelId = channelId;
  }

  public int getCustomerId()
  {
    return customerId;
  }

  public void setCustomerId(int customerId)
  {
    this.customerId = customerId;
  }

  public int getProductId()
  {
    return productId;
  }

  public void setProductId(int productId)
  {
    this.productId = productId;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  @Override public String toString()
  {
    return "SalesEvent{" +
        "timestamp=" + timestamp +
        ", productId=" + productId +
        ", customerId=" + customerId +
        ", channelId=" + channelId +
        ", regionId=" + regionId +
        ", productCategory=" + productCategory +
        ", amount=" + amount +
        ", discount=" + discount +
        ", tax=" + tax +
        '}';
  }
}
