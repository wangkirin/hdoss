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

	// �������ļ�
	public static void createFile(String dst, byte[] contents) throws IOException {
		FileSystem fs = getFileSystem(conf);
		Path dstPath = new Path(dst); // Ŀ��·��
		// ��һ�������
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(contents);
		outputStream.close();
		fs.close();
		System.out.println("�ļ������ɹ���");
	}

	// �ϴ������ļ�
	public static void uploadFile(String src, String dst) throws IOException {
		FileSystem fs = getFileSystem(conf);
		Path srcPath = new Path(src); // ԭ·��
		Path dstPath = new Path(dst); // Ŀ��·��
		// �����ļ�ϵͳ���ļ����ƺ���,ǰ�������ָ�Ƿ�ɾ��ԭ�ļ���trueΪɾ����Ĭ��Ϊfalse
		fs.copyFromLocalFile(false, srcPath, dstPath);

		// ��ӡ�ļ�·��
		System.out.println("Upload to " + conf.get("fs.default.name"));
		System.out.println("------------list files------------" + "\n");
		FileStatus[] fileStatus = fs.listStatus(dstPath);
		for (FileStatus file : fileStatus) {
			System.out.println(file.getPath());
		}
		fs.close();
	}

	// �ļ�������
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

	// ɾ���ļ�
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

	// ����Ŀ¼
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

	// ��ȡ�ļ�������
	public static void readFile(String filePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(filePath);
		InputStream in = null;
		try {
			in = fs.open(srcPath);
			IOUtils.copyBytes(in, System.out, 4096, false); // ���Ƶ���׼�����
		} finally {
			// IOUtils.closeStream(in);
		}
	}

	public static void main(String[] args) throws IOException {

		System.setProperty("hadoop.home.dir", "E:\\hadoop");

		// �����ϴ��ļ�
		// uploadFile("E:\\1.txt", "/user/hadoop/test/1.txt");

		// ���Դ����ļ�
		// byte[] contents = "hello �������\n".getBytes();
		// createFile("/user/hadoop/test1/d.txt",contents);

		// *����������
//		 rename("/user/hadoop/test1/d.txt", "/user/hadoop/test/dd.txt");

		// ����ɾ���ļ�
		// delete("/user/hadoop/test"); //ʹ�����·��
		// delete("test1"); //ɾ��Ŀ¼

		// �����½�Ŀ¼
		// mkdir("/user/hadoop/test");
		//
		// // ���Զ�ȡ�ļ�
		// readFile("/test/testfile.txt");

	}

}