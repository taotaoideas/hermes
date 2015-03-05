package com.ctrip.hermes.local.pojo;

public class Order {

	private String id;

	private double price;

	public Order() {
	}

	public Order(String id, double price) {
		this.id = id;
		this.price = price;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@Override
	public String toString() {
		return "Order [id=" + id + ", price=" + price + "]";
	}

}
