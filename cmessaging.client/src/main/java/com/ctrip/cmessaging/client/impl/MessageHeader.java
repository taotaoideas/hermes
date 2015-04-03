package com.ctrip.cmessaging.client.impl;

import java.util.HashMap;

public class MessageHeader {
	
	private final Config appconfig = new Config();
	
	private String CorrelationID = "";
	  
	private String Sequence = "1";
      
	private String AppID = appconfig.getAppId();
      
	private HashMap<String, String> UserHeader = new HashMap<String, String>();

	public String getCorrelationID() {
		return CorrelationID;
	}

	public void setCorrelationID(String correlationID) {
		CorrelationID = correlationID;
	}

	public String getSequence() {
		return Sequence;
	}

	public void setSequence(String sequence) {
		Sequence = sequence;
	}

	public String getAppID() {
		return AppID;
	}

	public void setAppID(String appID) {
		AppID = appID;
	}

	public HashMap<String, String> getUserHeader() {
		return UserHeader;
	}

	public void setUserHeader(HashMap<String, String> userHeader) {
		UserHeader = userHeader;
	}
      
}
