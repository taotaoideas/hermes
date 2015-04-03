package com.ctrip.cmessaging.client.impl;

import java.util.Properties;

/** 
 * @author Phxydown E-mail: 264162213@qq.com
 * @version build-time: 2014-5-26
 */
public class Config {
	
	Properties properties = new Properties();
	
	private static String appId = "555555";
	
	private static String platform = "java";
	
	private static String clientVersion = "java-client-1.0.1";
	
	private static String ackBufferEndurance = "20";
	
	private static String ackBufferTimeout = "15";
	
	/**
	 *  FWS:http://ws.config.framework.fws.qa.nt.ctripcorp.com/Configws/ServiceConfig/ConfigInfoes/Get/
	 *  UAT:http://ws.config.framework.uat.qa.nt.ctripcorp.com/Configws/ServiceConfig/ConfigInfoes/Get/
	 *  PRD:http://ws.config.framework.sh.ctripcorp.com/Configws/ServiceConfig/ConfigInfoes/Get/
	 *  Default to FWS
	 */
	private static String configWsUri = "http://ws.config.framework.fws.qa.nt.ctripcorp.com/Configws/ServiceConfig/ConfigInfoes/Get/";
	
	private String _appId;
	
	private String _platform;
	
	private String _clientVersion;
	
	private String _ackBufferEndurance;
	
	private String _ackBufferTimeout;
	
	private String _configWsUri;
	
	public Config(){
		this._appId = appId;
		this._platform = platform;
		this._clientVersion = clientVersion;
		this._ackBufferEndurance = ackBufferEndurance;
		this._ackBufferTimeout = ackBufferTimeout;
		this._configWsUri = configWsUri;
	}

	public String getAppId() {
		return _appId;
	}

	public String getPlatform() {
		return _platform;
	}
	
	public String getClientVersion() {
		return _clientVersion;
	}
	public String getAckBufferEndurance() {
		return _ackBufferEndurance;
	}

	public String getAckBufferTimeout() {
		return _ackBufferTimeout;
	}
	
	public String getConfigWsUri() {
		return _configWsUri;
	}
	
	
	public static void setAppId(String appId) {
		Config.appId = appId;
	}
	
	public static void setAckBufferEndurance(String ackBufferEndurance) {
		Config.ackBufferEndurance = ackBufferEndurance;
	}

	public static void setAckBufferTimeout(String ackBufferTimeout) {
		Config.ackBufferTimeout = ackBufferTimeout;
	}
	
	public static void setConfigWsUri(String configWsUri) {
		Config.configWsUri = configWsUri;
	}
}
