package org.apache.hbase.work;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class table {

	public static void main(String[] args) {
		char[] fir_name = {'a','b','c','d','e','f','g','h','i','j','k','l','m'};
		char[] sec_name = {'n','o','p','q','r','s','t','u','v','w','x','y','z'};
		char[] sex = {'m','w'};
		String[] subjects = {"IOT","MATH","ENGLISH","COMPUTING"};
		String[] provinces = {"AAA","BBB","CCC","DDD","EEE","FFF","GGG","HHH","III","JJJ","KKK"};
		String[] citys = {"aa","bb","cc","dd","ee","ff","gg","hh","ii","jj"};
		String[] streets = {"1","2","3","4","5","6"};
		Random r = new Random();
		File dataFile = new File("data.txt");
		try{
			if(!dataFile.exists())
				dataFile.createNewFile();			
		}
		catch (Exception e){
			e.printStackTrace();
		}
		for(int index_sub = 0;index_sub<4;index_sub++){
			for(int index_year = 2013;index_year<2018;index_year++){
				for(int index_class = 1;index_class<3;index_class++){
					for(int i=1;i<40;i++){
						try {
							FileWriter fw = new FileWriter(dataFile, true);
							fw.write(i+"\t");
							int fir_num = r.nextInt(2)+1;
							int sec_num = r.nextInt(2)+1;
							for(int fir = 0;fir<fir_num;fir++){
								int i_fir = r.nextInt(13);
								fw.write(fir_name[i_fir%13]);
							}
							fw.write("_");
							for(int sec=0;sec<sec_num;sec++){
								int i_sec = r.nextInt(13);
								fw.write(sec_name[i_sec%13]);
							}
							fw.write("\t");
							int i_sex = r.nextInt(1);
							fw.write(sex[i_sex]+"\t");
							int year = r.nextInt(3)+1995;
							int month = r.nextInt(12)+1;
							int day = r.nextInt(30)+1;
							fw.write(year+"-"+month+"-"+day+"\t");
							fw.write(subjects[index_sub]+"-"+index_year+"-"+index_class+"\t");
							int i_pro = r.nextInt(11);
							int i_city = r.nextInt(10);
							int i_street = r.nextInt(6);
							fw.write(provinces[i_pro]+"-"+citys[i_city]+"-"+streets[i_street]+"\t");
							int index = r.nextInt(3)+2;
							for(int j=0;j<index;j++){
								fw.write(r.nextInt(100)+"");
								if(j!=(index)-1)
									fw.write("-");
							}
							fw.write("\n");
							fw.flush();
							fw.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
				}
			}
		}
	}
}
