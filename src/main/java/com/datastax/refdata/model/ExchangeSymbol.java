package com.datastax.refdata.model;

public class ExchangeSymbol {

	private String exchange;
	private String symbol;
	
	public ExchangeSymbol(String exchange, String symbol) {
		super();
		this.exchange = exchange;
		this.symbol = symbol;
	}
	public String getExchange() {
		return exchange;
	}
	public String getSymbol() {
		return symbol;
	}
	

}
