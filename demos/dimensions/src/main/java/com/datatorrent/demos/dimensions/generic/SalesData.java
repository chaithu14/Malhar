package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.DTThrowable;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SalesData
{

  public long timestamp;
  public int productId;
  public int customerId;
  public int channelId;
  public int regionId;
  public int productCategory;

  public double amount;
  public double discount;
  public double tax;

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  public int getProductId()
  {
    return productId;
  }

  public void setProductId(int productId)
  {
    this.productId = productId;
  }

  public int getCustomerId()
  {
    return customerId;
  }

  public void setCustomerId(int customerId)
  {
    this.customerId = customerId;
  }

  public int getChannelId()
  {
    return channelId;
  }

  public void setChannelId(int channelId)
  {
    this.channelId = channelId;
  }

  public int getRegionId()
  {
    return regionId;
  }

  public void setRegionId(int regionId)
  {
    this.regionId = regionId;
  }

  public double getAmount()
  {
    return amount;
  }

  public void setAmount(double amount)
  {
    this.amount = amount;
  }

  public double getDiscount()
  {
    return discount;
  }

  public void setDiscount(double discount)
  {
    this.discount = discount;
  }

  public double getTax()
  {
    return tax;
  }

  public void setTax(double tax)
  {
    this.tax = tax;
  }

  public int getProductCategory()
  {
    return productCategory;
  }

  public void setProductCategory(int productCategory)
  {
    this.productCategory = productCategory;
  }

  @Override public String toString()
  {
    return "SalesData{" +
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

