package com.huawei.containerops.hdoss.services;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.huawei.obs.services.exception.ObsException;
import com.huawei.obs.services.model.ListObjectsRequest;
import com.huawei.obs.services.model.ObjectListing;
import com.huawei.obs.services.model.S3Object;

public class Test {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "E:\\hadoop");
		ObsConfiguration obsconf = new ObsConfiguration();
		obsconf.setEndPoint("hdfs://10.229.40.121:9000");

		// test list objects
		try {
			ObsClient obscli = new ObsClient("", "", "", obsconf);
			ListObjectsRequest listrequest = new ListObjectsRequest();
			listrequest.setBucketName("test/");
			ObjectListing objlist = obscli.listObjects(listrequest);
			if (null!=objlist) {
				List<S3Object> s3s = objlist.getObjectSummaries();
				for (S3Object s3Object : s3s) {
					System.out.println(s3Object.getObjectKey());
				}
			}
			
		} catch (ObsException e) {
			e.printStackTrace();
		}

		// test create bucket
		try {
			ObsClient obscli = new ObsClient("", "", "", obsconf);
			obscli.createBucket("testhdoss1", "chn");
			System.out.println("create bucket OK");
		} catch (ObsException e) {
			e.printStackTrace();
		}

		// test get object
		try {
			ObsClient obscli = new ObsClient("", "", "", obsconf);
			S3Object s3 = obscli.getObject("test", "testfile.txt", null);
			if (null != s3) {
				InputStream is = s3.getObjectContent();
				int tempbyte;
				try {
					while ((tempbyte = is.read()) != -1) {
						System.out.write(tempbyte);
					}
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		} catch (ObsException e) {
			e.printStackTrace();
		}

		// test put object
		ObsClient obscli;
		try {
			obscli = new ObsClient("", "", "", obsconf);
			File putfile = new File("E:\\astrill-setup-win.exe");
			obscli.putObject("test2", "docker1.pdf", putfile);
		} catch (ObsException e) {
			e.printStackTrace();
		}

	}
}
