package cn.edu.ruc.biz;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import cn.edu.ruc.db.DBBase;

/**
 * demo
 * @author sxg
 */
public class Demo {
	
	
	//1,写入吞吐量测试
	//方法1,现有的,每次缓存一定条数写入
	//方法2,直接生成一个附件,调用import命令导入
	//方法1已经完成，方法2需要继续修改程序，方法不知道数据库内部机制如何运行的
	
	
	//2，吞吐量测试， 混合请求   参数可配置，根据cassandra的特点，可以减少开始的请求数(或者直接加特殊标志)
	//计算方式  qps(tps)=总请求数/平均响应时间
	//需要有一个加压过程，
	//开始1s一个请求，迭代累加1000，加到 10000,查看测试结果
	//利用线程数控制数据库服务器每次的并发请求数
	//每次常开 50000个连接
	static int count=0;
	public static  void requestThroughput(DBBase db){
		int sumCount=10002;
		int currentCount=1;
		while(currentCount<sumCount){
			count=0;
			//发送currentCount 个请求
			ExecutorService pool = Executors.newFixedThreadPool(currentCount);
			for(int i=0;i<currentCount;i++){
				pool.execute(new Runnable() {
					@Override
					public void run() {
						try {
							Thread.sleep(1000L);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						System.out.println(count++);
					}
				});
			}
			pool.shutdown();
			try {
				pool.awaitTermination(1, TimeUnit.HOURS);
				System.out.println("currentCount"+currentCount);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			currentCount+=1000;
		}
	}
	public static void main(String[] args) {
		requestThroughput(null);
	}
	
	
}

