package com.huawei.containerops;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class TestHDFS {
	private static Configuration conf;
	static {
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.229.40.121:9000");
		// conf.addResource(new Path("E:\\hdfs\\core-site.xml"));
		// conf.addResource(new Path("E:\\hdfs\\hdfs-site.xml"));
	}

	private static FileSystem getFileSystem(final Configuration clientconf) {
		FileSystem handlerfs = null;
		String user = "hdfs";
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

	// 创建新文件
	public static void createFile(String dst, byte[] contents) throws IOException {
		FileSystem fs = getFileSystem(conf);
		Path dstPath = new Path(dst); // 目标路径
		// 打开一个输出流
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(contents);
		outputStream.close();
		fs.close();
		System.out.println("文件创建成功！");
	}

	// 上传本地文件
	public static void uploadFile(String src, String dst) throws IOException {
		FileSystem fs = getFileSystem(conf);
		Path srcPath = new Path(src); // 原路径
		Path dstPath = new Path(dst); // 目标路径
		// 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
		fs.copyFromLocalFile(false, srcPath, dstPath);

		// 打印文件路径
		System.out.println("Upload to " + conf.get("fs.default.name"));
		System.out.println("------------list files------------" + "\n");
		FileStatus[] fileStatus = fs.listStatus(dstPath);
		for (FileStatus file : fileStatus) {
			System.out.println(file.getPath());
		}
		fs.close();
	}

	// 文件重命名
	public static void rename(String oldName, String newName) throws IOException {
		FileSystem fs = getFileSystem(conf);
		Path oldPath = new Path(oldName);
		Path newPath = new Path(newName);
		boolean isok = fs.rename(oldPath, newPath);
		if (isok) {
			System.out.println("rename ok!");
		} else {
			System.out.println("rename failure");
		}
		fs.close();
	}

	// 删除文件
	public static void delete(String filePath) throws IOException {
		FileSystem fs = getFileSystem(conf);
		Path path = new Path(filePath);
		boolean isok = fs.deleteOnExit(path);
		if (isok) {
			System.out.println("delete ok!");
		} else {
			System.out.println("delete failure");
		}
		fs.close();
	}

	// 创建目录
	public static void mkdir(String path) throws IOException {
		FileSystem fs = getFileSystem(conf);
		Path srcPath = new Path(path);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			System.out.println("create dir ok!");
		} else {
			System.out.println("create dir failure");
		}
		fs.close();
	}

	// 读取文件的内容
	public static void readFile(String filePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(filePath);
		InputStream in = null;
		try {
			in = fs.open(srcPath);
			IOUtils.copyBytes(in, System.out, 4096, false); // 复制到标准输出流
		} finally {
			// IOUtils.closeStream(in);
		}
	}

	public static void main(String[] args) throws IOException {

		System.setProperty("hadoop.home.dir", "E:\\hadoop");

		// 测试上传文件
		// uploadFile("E:\\1.txt", "/user/hadoop/test/1.txt");

		// 测试创建文件
		// byte[] contents = "hello 世界你好\n".getBytes();
		// createFile("/user/hadoop/test1/d.txt",contents);

		// *测试重命名
//		 rename("/user/hadoop/test1/d.txt", "/user/hadoop/test/dd.txt");

		// 测试删除文件
		// delete("/user/hadoop/test"); //使用相对路径
		// delete("test1"); //删除目录

		// 测试新建目录
		// mkdir("/user/hadoop/test");
		//
		// // 测试读取文件
		// readFile("/test/testfile.txt");

	}

}