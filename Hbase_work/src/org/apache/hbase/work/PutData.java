package org.apache.hbase.work;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.jruby.RubyProcess.Sys;

public class PutData {

	
	private static final String TABLE_NAME = "test";
	private static final String CF_DEFAULT = "cf";
	
	public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
	  if (admin.tableExists(table.getTableName())) {
	    admin.disableTable(table.getTableName());
	    admin.deleteTable(table.getTableName());
	  }
	  admin.createTable(table);  
	}
	
  public static void createSchemaTables(Configuration config) throws IOException {
	    try (Connection connection = ConnectionFactory.createConnection(config);
	         Admin admin = connection.getAdmin()) {

	      HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
	      table.addFamily(new HColumnDescriptor("StuNo").setCompressionType(Algorithm.NONE));
	      table.addFamily(new HColumnDescriptor("name").setCompressionType(Algorithm.NONE));
	      table.addFamily(new HColumnDescriptor("sex").setCompressionType(Algorithm.NONE));
	      table.addFamily(new HColumnDescriptor("birthday").setCompressionType(Algorithm.NONE));
	      table.addFamily(new HColumnDescriptor("subject").setCompressionType(Algorithm.NONE));
	      table.addFamily(new HColumnDescriptor("address").setCompressionType(Algorithm.NONE));
	      table.addFamily(new HColumnDescriptor("score").setCompressionType(Algorithm.NONE));

	      System.out.print("Creating table. ");
      createOrOverwrite(admin, table);
      System.out.println(" Done.");
    }
  }
	private static Configuration config;
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	
	    config = HBaseConfiguration.create();
	
	    //Add any necessary configuration files (hbase-site.xml, core-site.xml)
	//	    config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
	//	    config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
	    
	    config.addResource("/home/hadoop/hbase/conf/hbase-site.xml");
	    config.addResource("/home/hadoop/hbase/conf/core-site.xml");
		createSchemaTables(config);
	    
		FileReader reader;
		try {
			reader = new FileReader("/home/hadoop/workspace/Hbase_work/data.txt");
			BufferedReader br = new BufferedReader(reader);
			String line = null;
			int n = 1;
			while((line = br.readLine())!=null){
//				System.out.println(line);
				Save(line, n);
				n++;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
	}
	
	@SuppressWarnings("deprecation")
	private static void Save(String line, int n) throws IOException {
		// TODO Auto-generated method stub
		String[] kinds = line.split("\t");
		
		String stuNo = kinds[0];
//		System.out.println(kinds.length);
		
		String[] name = kinds[1].split("_");
//		System.out.print(kinds[1]+" "+name.length+" ");
		String firName = name[0];
		String secName = name[1];
		
		String sex = kinds[2];
		
		String[] birth = kinds[3].split("-");
//		System.out.print(birth.length+" ");
		String birYear = birth[0];
		String birMon = birth[1];
		String birDay = birth[2];
		
		String[] subject = kinds[4].split("-");
//		System.out.print(subject.length+" ");

		String subName = subject[0];
		String subYear = subject[1];
		String subClass = subject[2];
		
		String[] address = kinds[5].split("-");
//		System.out.print(address.length+" ");

		String provience = address[0];
		String city = address[1];
		String street = address[2];
		
		String[] scores = kinds[6].split("-");
//		System.out.print(scores.length+" ");

		
//		System.out.print(stuNo+" "+firName+" "+secName+" "
//				+birYear+" "+birMon+" "+birDay+" "
//				+subName+" "+subYear+" "+subClass+" "
//				+provience+" "+city+" "+street+" ");
//		for(int i = 0;i<scores.length;i++){
//			System.out.print(scores[i]+" ");
//		}
//		System.out.println();
	    HTable table = new HTable(config, "test");
		String rowNum = "row"+n;
		System.out.println(rowNum);
		Put putRow1 = new Put(rowNum.getBytes());
		putRow1.add("StuNo".getBytes(), "StuNo".getBytes(), stuNo.getBytes());
		
		putRow1.add("name".getBytes(), "FirName".getBytes(), firName.getBytes());
		putRow1.add("name".getBytes(), "SecName".getBytes(), secName.getBytes());
		
		putRow1.add("sex".getBytes(), "sex".getBytes(), sex.getBytes());
		
		putRow1.add("birthday".getBytes(), "birYear".getBytes(), birYear.getBytes());
		putRow1.add("birthday".getBytes(), "birMonth".getBytes(), birMon.getBytes());
		putRow1.add("birthday".getBytes(), "birDay".getBytes(), birDay.getBytes());
		
		putRow1.add("subject".getBytes(), "subName".getBytes(), subName.getBytes());
		putRow1.add("subject".getBytes(), "subYear".getBytes(), subYear.getBytes());
		putRow1.add("subject".getBytes(), "subClass".getBytes(), subClass.getBytes());
		
		putRow1.add("address".getBytes(), "provience".getBytes(), provience.getBytes());
		putRow1.add("address".getBytes(), "city".getBytes(), city.getBytes());
		putRow1.add("address".getBytes(), "street".getBytes(), street.getBytes());
		
		for(int i=0;i<scores.length;i++){
			String scoreName = "score"+(i+1);
			putRow1.add("score".getBytes(), scoreName.getBytes(), scores[i].getBytes());
		}
		
		table.put(putRow1);

		System.out.println("Done.");
		
		
	//		Put put = new Put();
	}

}
