package cn.edu.ruc.biz;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.rmi.CORBA.Tie;

import cn.edu.ruc.TSUtils;
import cn.edu.ruc.TimeSlot;
import cn.edu.ruc.biz.model.LoadRatio;
import cn.edu.ruc.biz.model.ReadRecord;
import cn.edu.ruc.biz.model.TimeoutRecord;
import cn.edu.ruc.db.DBBase;
import cn.edu.ruc.db.Status;
import cn.edu.ruc.db.TsPoint;
import cn.edu.ruc.enums.LoadTypeEnum;
import cn.edu.ruc.enums.ReadTypeEnum;
/**
 * 标准测试版本1
 * 根据函数比例进行
 */
public class ClientThreadSTV1 implements Runnable {
	private  boolean spinSleep=true;
	private  boolean isPrint=false;//是否打印进度

	private TimeoutRecord record;
	private int loadType;//1 单操作  2混合操作
	private DBBase db;
	// 目标执行总次数
	private int opcount;
	// 目标每毫秒的执行数
	private double targetOpsPerMs;
	// 执行次数
	private int opsdone;
	// tick 每隔多少纳秒执行一次
	private long targetOpsTickNs;

	//各个负载比例
	private LoadRatio loadRatio;
	Random random=new Random();
	@Override
	public void run() {
		long startTimeNanos = System.nanoTime();
		while (opsdone < opcount) {
			// 业务
			try {
				Integer executeType=0;//当前线程的操作类型
				if(LoadTypeEnum.MUILTI.getId().equals(this.loadType)){
					double rd = random.nextDouble();
					if(rd>=loadRatio.getWriteStartRatio()&&rd<loadRatio.getWriteEndRatio()){
						executeType=LoadTypeEnum.WRITE.getId();
					}
					if(rd>=loadRatio.getRandomInsertStartRatio()&&rd<loadRatio.getRandomInsertEndRatio()){
						executeType=LoadTypeEnum.RANDOM_INSERT.getId();
					}
					if(rd>=loadRatio.getUpdateStartRatio()&&rd<loadRatio.getUpdateEndRatio()){
						executeType=LoadTypeEnum.UPDATE.getId();
					}
					if(rd>=loadRatio.getSimpleQueryStartRatio()&&rd<loadRatio.getSimpleQueryEndRatio()){
						executeType=LoadTypeEnum.SIMPLE_READ.getId();
					}
					if(rd>=loadRatio.getAggrQueryStartRatio()&&rd<loadRatio.getAggrQueryEndRatio()){
						executeType=LoadTypeEnum.AGGRA_READ.getId();
					}
				}else{
					executeType=loadType;
				}
				try {
					Status status = execQueryByReadType(db, executeType);
					if(status.isOK()){
						record.addSuccessTimes(executeType, status.getCostTime());
					}else{
						record.addFailedTimes();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			printProgress();
			opsdone++;
			throttleNanos(startTimeNanos);//FIXME 暂时没用
		}
	}
	private  Status execQueryByReadType(DBBase dbBase,Integer loadType) throws Exception{
		Status status=null;
		TsPoint point=new TsPoint();
		Integer internal=24*60;//一天
		String deviceCode = Core.getDeviceCodeByRandom();
		String sensorCode = Core.getSensorCodeByRandom();
		point.setDeviceCode(deviceCode);
		point.setSensorCode(sensorCode);
		//FIXME 查询时间段可优化
		if(LoadTypeEnum.WRITE.getId().equals(loadType)){
			//选择当前时间 插入数据
			List<TsPoint> points = Core.generateInsertData(1);
			status = dbBase.insertMulti(points);
		}
		if(LoadTypeEnum.RANDOM_INSERT.getId().equals(loadType)){
			//随机选择15分钟   HISTORY_START_TIME之前的 
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(TSUtils.getTimeByDateStr("2016-02-15 00:00:00"),Constants.HISTORY_START_TIME,TimeUnit.SECONDS.toMillis(6));
			//生成数据6s，进行写入
			List<TsPoint> points = generateDataBetweenTime(timeSlot.getStartTime(),timeSlot.getEndTime());
			status=dbBase.insertMulti(points);
		}
		if(LoadTypeEnum.UPDATE.getId().equals(loadType)){
			//随机选择6s   -HISTORY_START_TIME 取模为0的
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME,Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(6));//时间可调整
			List<TsPoint> points = generateDataBetweenTime(timeSlot.getStartTime(),timeSlot.getEndTime());
			status=dbBase.updatePoints(points);
		}
		if(LoadTypeEnum.SIMPLE_READ.getId().equals(loadType)){
			//查一天的数据
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(3600));
			status = dbBase.selectByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(LoadTypeEnum.AGGRA_READ.getId().equals(loadType)){
			//查一天的数据的最大值
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(3600));
			status = dbBase.selectMaxByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		return status;
	}
	/**
	 * 生成某个时间段内的数据
	 * @param startTime
	 * @param endTime
	 * @return
	 * @deprecated 由core替代
	 */
	private  List<TsPoint> generateDataBetweenTime(long startTime,long endTime){
		//单线程生成
		List<TsPoint> points=new ArrayList<TsPoint>();
		int deviceSum=Constants.DEVICE_NUMBER;
		int sensorSum=Constants.SENSOR_NUMBER;
		long step=Constants.POINT_STEP;
		double loseRatio=Constants.POINT_LOSE_RATIO;
		long current=0;
		Random r=new Random();
		for(long currentTime=startTime;currentTime<=endTime;){
			for(int deviceNum=0;deviceNum<deviceSum;deviceNum++){
				String deviceCode=Constants.DEVICE_CODES.get(deviceNum);
				for(int sensorNum=0;sensorNum<sensorSum;sensorNum++){
					double randomFloat = r.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = Constants.SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(Core.getValue(deviceCode,sensorCode,currentTime));
						point.setTimestamp(currentTime);
						points.add(point);
					}
					current++;
					if(current%100000==0){
						System.out.println(current);
					}
				}
			}
			currentTime+=step;
		}
		return points;
	}
	/**
	 * 
	 * @param record 
	 * @param loadType  LoadTypeEnum 
	 * @param db  
	 * @param opcount 
	 * @param targetOpsPerMs 
	 * @param loadRatio 
	 */
	public ClientThreadSTV1(TimeoutRecord record,Integer loadType, DBBase db, int opcount,
			double targetOpsPerMs/*,LoadRatio loadRatio*/) {
//		this.loadRatio=loadRatio;
		this.record = record;
		this.loadType=loadType;//
		this.db = db;
		this.opcount = opcount;
		this.targetOpsPerMs = targetOpsPerMs;
		this.opsdone = 0;
		if (targetOpsPerMs > 0) {
			this.targetOpsPerMs = targetOpsPerMs;
			this.targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
		}
		this.loadRatio=LoadRatio.newInstanceByLoadType(loadType);
	}
	public ClientThreadSTV1(TimeoutRecord record,Integer loadType, DBBase db, int opcount,
			double targetOpsPerMs,boolean spinSleep) {
//		this.loadRatio=loadRatio;
		this.spinSleep=spinSleep;
		this.record = record;
		this.loadType=loadType;//
		this.db = db;
		this.opcount = opcount;
		this.targetOpsPerMs = targetOpsPerMs;
		this.opsdone = 0;
		if (targetOpsPerMs > 0) {
			this.targetOpsPerMs = targetOpsPerMs;
			this.targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
		}
		this.loadRatio=LoadRatio.newInstanceByLoadType(loadType);
	}
	public ClientThreadSTV1(TimeoutRecord record,Integer loadType, DBBase db, int opcount,
			double targetOpsPerMs,boolean spinSleep,boolean isPrint) {
//		this.loadRatio=loadRatio;
		this.isPrint=isPrint;
		this.spinSleep=spinSleep;
		this.record = record;
		this.loadType=loadType;//
		this.db = db;
		this.opcount = opcount;
		this.targetOpsPerMs = targetOpsPerMs;
		this.opsdone = 0;
		if (targetOpsPerMs > 0) {
			this.targetOpsPerMs = targetOpsPerMs;
			this.targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
		}
		this.loadRatio=LoadRatio.newInstanceByLoadType(loadType);
	}

	private void throttleNanos(long startTimeNanos) {
		// throttle the operations
		if (targetOpsPerMs > 0) {
			// delay until next tick
			long deadline = startTimeNanos + opsdone * targetOpsTickNs;
			sleepUntil(deadline);
		}
	}

	private  void sleepUntil(long deadline) {
//		System.out.println(deadline - System.nanoTime());
		while (System.nanoTime() < deadline) {
			if (!spinSleep) {
				LockSupport.parkNanos(deadline - System.nanoTime());
			}
		}
	}
	private int currentPercent=0;
	private void printProgress(){
		if(isPrint){
			int percent=(int)((double)opsdone/opcount*100);
			if(percent!=currentPercent){
				if(currentPercent==0){
					System.out.println("");
				}else{
					System.out.print("\b\b\b");
				}
				if(currentPercent>9){
					System.out.print("\b");
				}
				System.out.print("=>"+percent+"%");
				currentPercent=percent;
				if(currentPercent==99){
					System.out.println("");
				}
			}
		}
	}
}
