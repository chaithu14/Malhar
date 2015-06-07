package com.datatorrent.demos.dimensions.generic;

/**
 * A single sales event
 */
public class ProductEvent {

  public int productId;
  public int productCategory;

  public int getProductId()
  {
    return productId;
  }

  public void setProductId(int productId)
  {
    this.productId = productId;
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
    return "ProductEvent{" +
        "productId=" + productId +
        ", productCategory=" + productCategory +
        '}';
  }
}
