package com.huodian.hs4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.huodian.hs4j.command.*;
import com.huodian.hs4j.core.HSIndexDescriptor;
import com.huodian.hs4j.core.HSProto;
import com.huodian.hs4j.result.HSResult;
import com.huodian.hs4j.result.HSResultFuture;

public class HSBenchMark {
    private static int N = 2000;
    private static int THREAD_NUM = 4;
    private static int POOLSIZE = 120;
	private static int LOOP = 10;
	private static String testItem = "insert|update|query|delete";
	
    private static HSIndexDescriptor updateIndexDescr;
    private static HSIndexDescriptor insertIndexDescr;
    private static HSManager hsm;
    
    private static int wrongNum = 0;
    
	public static void main(String[] args) throws Exception {
		//threadNum, poolSize, count, loop
		if(args.length > 0) {
			THREAD_NUM = Integer.valueOf(args[0]);
		}
		
		if(args.length > 1) {
			POOLSIZE = Integer.valueOf(args[1]);
		}
		
		if(args.length > 2) {
			N = Integer.valueOf(args[2]);
		}
		
		if(args.length > 3) {
			LOOP = Integer.valueOf(args[3]);
		}
		
		if(args.length > 4) {
			testItem = args[4].toLowerCase();
		}
		
		hsm = new HSManager("192.168.22.55", "123456", POOLSIZE, Runtime.getRuntime().availableProcessors());

		updateIndexDescr = new HSIndexDescriptor("waf", "t_user", "PRIMARY", new String[]{"email", "password", "createTime"});
		int sn = hsm.openIndex(updateIndexDescr);
		if(sn > 0) {
		    System.out.println("Fail to open index on connection " + sn);
		    System.exit(0);
		}
		
		insertIndexDescr = new HSIndexDescriptor("waf", "t_user", "PRIMARY", new String[]{"account", "email", "password", "createTime"});
		sn = hsm.openIndex(insertIndexDescr);
		if(sn > 0) {
		    System.out.println("Fail to open index on connection " + sn);
		    System.exit(0);
		}
		
		long t0 = System.currentTimeMillis();
	    final CountDownLatch counter = new CountDownLatch(THREAD_NUM);
	    ExecutorService exectors = Executors.newFixedThreadPool(THREAD_NUM);
	    
		for(int i = 0; i < THREAD_NUM; i++) {
			final int nn = i;
			
		    exectors.execute(new Runnable() {
		        public void run() {
		            System.out.println("Start");
		            String n = "a" + (nn * N);
		            
		            if(testItem.indexOf("insert") >=0) {
			            for(int i = 0; i < N; i++) {
			                try {
			                    testInsert((n + i) + '_' + "%03d");
			                } catch(Exception e) {
			                    e.printStackTrace();
			                }
			            }
		            }
		            
		            if(testItem.indexOf("update") >=0) {
			            for(int i = 0; i < N; i++) {
			                try {
			                    testUpdate((n + i) + '_' + "%03d");
			                } catch(Exception e) {
			                    e.printStackTrace();
			                }
			            }
		            }
		            
		            if(testItem.indexOf("query") >=0) {
			            for(int i = 0; i < N; i++) {
			                try {
			                    testQuery((n + i) + '_' + "%03d");
			                } catch(Exception e) {
			                    e.printStackTrace();
			                }
			            }
			            System.out.println("Wrong num " + wrongNum);
		            }
		            
		            if(testItem.indexOf("delete") >=0) {
			            for(int i = 0; i < N; i++) {
			                try {
			                    testDelete((n + i) + '_' + "%03d");
			                } catch(Exception e) {
			                    e.printStackTrace();
			                }
			            }
		            }
                    System.out.println("End");
					counter.countDown();
		        }
		    });
		}

		counter.await();
        hsm.close();

        long t = System.currentTimeMillis() - t0;
		System.out.println("Speed = " + (1000L * N * THREAD_NUM / t));
		System.exit(0);
	}
	
	private static final void testInsert(String format) throws SQLException {
        HSResultFuture resultFuture;
        HSResult[] results;
        HSCommand[] cmds = new HSCommand[LOOP];
        String s;
        
        for(int i = 0; i < LOOP; i++) {
        	s = String.format(format, i);
        	cmds[i] = new HSInsert(new String[]{s, s + "@xxx.com", s, "2014-01-01"});
        }
        resultFuture = hsm.execute(insertIndexDescr, cmds);
        results = resultFuture.get();
        printResults(results, insertIndexDescr);
	}
	
	private static final void testUpdate(String format) throws SQLException {
        HSResultFuture resultFuture;
        HSResult[] results;
        HSCommand[] cmds = new HSCommand[LOOP];
        String s;
        
        for(int i = 0; i < LOOP; i++) {
        	s = String.format(format, i);
        	cmds[i] = new HSModify(CompareOperator.EQ, new String[]{s}, new String[]{"n" + s + "@xxx.com", s});
        }
        resultFuture = hsm.execute(updateIndexDescr, cmds);
        results = resultFuture.get();
        printResults(results, updateIndexDescr);
	}
	
	private static final void testQuery(String format) throws SQLException {
        HSResultFuture resultFuture;
        HSResult[] results;
        String s;
        
        for(int i = 0; i < LOOP; i++) { //cann't use batch
        	s = String.format(format, i);
	        resultFuture = hsm.execute(updateIndexDescr,
	            new HSCommand[] {
	                new HSFind(CompareOperator.EQ, new String[]{s}).limit(10)
	            }
	        );
	
	        results = resultFuture.get();
	        printResults(results, updateIndexDescr, s);
        }
	}

	private static final void testDelete(String format) throws SQLException {
        HSResultFuture resultFuture;
        HSResult[] results;
        HSCommand[] cmds = new HSCommand[LOOP];
        String s;
        
        for(int i = 0; i < LOOP; i++) {
        	s = String.format(format, i);
        	cmds[i] = new HSDelete(CompareOperator.EQ, new String[]{s});
        }
        resultFuture = hsm.execute(updateIndexDescr, cmds);
        results = resultFuture.get();
        printResults(results, updateIndexDescr);
	}
	
	private static void printResults(HSResult[] results, HSIndexDescriptor indexDescr) throws SQLException {
		printResults(results, indexDescr, null);
	}
	
	private static void printResults(HSResult[] results, HSIndexDescriptor indexDescr, String key) throws SQLException {
	    HSCommand cmd;
	    ResultSet resultSet;
	    
	    for(HSResult res : results) {
	        cmd = res.getCommand();
	        if(cmd.getStatus() == HSProto.STATUS_OK) {
		        if(cmd instanceof HSFind){
		            resultSet = ((HSFind)cmd).getResult();
		            if(resultSet != null) {
		            	if(!resultSet.next()) {
		                	wrongNum++;
		            		continue;
		            	}
		            	
		                if(resultSet.getString(1).indexOf(key) < 0) {
		                	wrongNum++;
		                    //System.out.println("wrong:" + resultSet.getString(1));
		                }
			        }
		        }
	            continue; //OK, needn't print it
	        }
	        
            System.out.println(cmd.toString());
	    }
	}
}