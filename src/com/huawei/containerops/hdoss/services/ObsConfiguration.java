package com.huawei.containerops.hdoss.services;

import org.apache.hadoop.conf.Configuration;

public class ObsConfiguration {
	private static String hdfsAddr;
	private Configuration conf;
	
	public ObsConfiguration() {
		conf = new Configuration();
	}
	
	public void setEndPoint(java.lang.String endPoint){
		hdfsAddr=endPoint;
		getConf().set("fs.defaultFS", hdfsAddr);
	}
	
	public void setHttpsOnly(boolean httpsOnly) {
		// void
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
