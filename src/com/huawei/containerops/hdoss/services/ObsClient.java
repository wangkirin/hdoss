package com.huawei.containerops.hdoss.services;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.huawei.obs.services.exception.ObsException;
import com.huawei.obs.services.model.S3Bucket;
import com.huawei.obs.services.model.S3BucketCors;
import com.huawei.obs.services.model.S3Object;
import com.huawei.obs.services.model.PutObjectResult;
import com.huawei.obs.services.model.ListObjectsRequest;
import com.huawei.obs.services.model.ObjectListing;
import com.huawei.obs.services.model.ObjectMetadata;

public class ObsClient {
	private static FileSystem fs;

	public ObsClient(String accessId, String accessKey, ObsConfiguration config) throws ObsException {
		if (accessId.equals("") && accessKey.equals("")) {
			fs = getFileSystem(config.getConf(), "hdfs");
		} else {
			// TODO 处理Kerberos鉴权凭证
		}
	}

	public S3Object getObject(String bucketName, String objectKey, String versionId) throws ObsException {
		Path srcPath = new Path("/"+bucketName+"/"+objectKey);
		S3Object outputobj=new S3Object();
		InputStream in = null;
			try {
				in = fs.open(srcPath);
				outputobj.setObjectContent(in);
			    ObjectMetadata objmeta=new ObjectMetadata();
			    objmeta.setContentLength((long) in.available());
			    outputobj.setMetadata(objmeta);
			} catch (Exception e) {
				e.printStackTrace();
				throw new ObsException(e.toString());
			}
		return outputobj;

	}

	public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws ObsException {
		String bucketName = listObjectsRequest.getBucketName();
		Path bucketPath = new Path("/" + bucketName);
		// TODO 入参校验，检查bucket是否存在
		ObjectListing outputlisting = new ObjectListing();
		List<S3Object> objectlist = new ArrayList<S3Object>();
		try {
			FileStatus status[] = fs.listStatus(bucketPath);
			for (FileStatus fileStatus : status) {
				S3Object s3obj = new S3Object();
				s3obj.setBucketName(bucketName);
				s3obj.setObjectKey(fileStatus.getPath().getName());
				objectlist.add(s3obj);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ObsException(e.toString());
		}
		outputlisting.setObjectSummaries(objectlist);
		return outputlisting;

	}

	public PutObjectResult putObject(String bucketName, String objectKey, File file) throws ObsException {
		// TODO 入参校验，检查bucket是否存在,检查object是否存在
		Path destPath = new Path("/" + bucketName + "/" + objectKey);

		try {
			FileInputStream fis = new FileInputStream(file);
			FSDataOutputStream outputStream = fs.create(destPath);
			byte[] tempbyte = new byte[100];
			int byteread = 0;
			while ((byteread = fis.read(tempbyte)) != -1) {
				outputStream.write(tempbyte);
			}
			outputStream.close();
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ObsException(e.toString());
		}

		return null;

	}

	public PutObjectResult putObject(String bucketName, String objectKey, InputStream input, ObjectMetadata metadata)
			throws ObsException {
		// TODO 入参校验，检查bucket是否存在,检查object是否存在
		Path destPath = new Path("/" + bucketName + "/" + objectKey);
		try {
			FSDataOutputStream outputStream = fs.create(destPath);
			byte[] tempbyte = new byte[100];
			int byteread = 0;
			while ((byteread = input.read(tempbyte)) != -1) {
				outputStream.write(tempbyte);
			}
			outputStream.close();
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ObsException(e.toString());
		}
		return null;
	}

	public S3Bucket createBucket(String bucketName, String location) throws ObsException {
		S3Bucket tempBucket = new S3Bucket();
		tempBucket.setBucketName(bucketName);
		tempBucket.setLocation(location);

		try {
			// TODO 检查bucket是否存在
			boolean ok = fs.mkdirs(new Path("/" + bucketName));
			if (!ok) {
				return null;
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new ObsException(e.toString());
		} 
		return tempBucket;
	}

	public void setBucketCors(java.lang.String bucketName, S3BucketCors s3BucketCors) throws ObsException {
		// void
	}

	private static FileSystem getFileSystem(final Configuration clientconf, String username) {
		FileSystem handlerfs = null;
		String user = username;
		try {
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);

			handlerfs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {

				@Override
				public FileSystem run() throws IOException {
					if (clientconf == null) {
						System.out.println("get hdfs configuration fail!");
						return null;
					}

					String user = "hdfs";
					clientconf.set("hadoop.job.ugi", user);

					try {
						return FileSystem.get(clientconf);
					} catch (IOException e) {
						System.out.println("init filesystemerror" + e.getMessage());
					}
					return null;
				}

			});

		} catch (IOException e) {
			System.out.println("getFileSystemError" + e.getMessage());
		} catch (InterruptedException e) {
			System.out.println("getFileSystemError" + e.getMessage());
		}
		return handlerfs;
	}

	private static FileSystem getSecurityFileSystem(final Configuration clientconf, UserGroupInformation ugi) {
		FileSystem handlerfs = null;
		try {
			if (ugi == null) {
				System.out.println("ugi==null");
				return null;
			}

			handlerfs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {

				@Override
				public FileSystem run() throws Exception {
					try {
						return FileSystem.get(clientconf);
					} catch (Exception e) {
						System.out.println("privilegedExceptionError" + e.getMessage());
					}
					return null;
				}

			});

		} catch (IOException e) {
			System.out.println("getFileSystemError" + e.getMessage());
		} catch (InterruptedException e) {
			System.out.println("getFileSystemError" + e.getMessage());
		}
		return handlerfs;
	}

}
