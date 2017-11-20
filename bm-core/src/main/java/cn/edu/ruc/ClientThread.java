package cn.edu.ruc;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import cn.edu.ruc.TSUtils;
import cn.edu.ruc.TimeSlot;
import cn.edu.ruc.biz.Constants;
import cn.edu.ruc.biz.Core;
import cn.edu.ruc.biz.FunctionParam;
import cn.edu.ruc.biz.model.ReadRecord;
import cn.edu.ruc.db.DBBase;
import cn.edu.ruc.db.Status;
import cn.edu.ruc.db.TsPoint;
import cn.edu.ruc.enums.ReadTypeEnum;

public class ClientThread implements Runnable {
	private  boolean spinSleep=false;

	private ReadRecord record;
	private int readType;
	private DBBase db;
	// private boolean dotransactions;
	// 目标执行总次数
	private int opcount;
	// 目标每毫秒的执行数
	private double targetOpsPerMs;
	// 执行次数
	private int opsdone;
	// tick 每隔多少纳秒执行一次
	private long targetOpsTickNs;

	@Override
	public void run() {
		long startTimeNanos = System.nanoTime();
		while (opsdone < opcount) {
			// 业务
			try {
				Status status = execQueryByReadType(db, readType);
				if(status.isOK()){
					record.addReadTypeTimes(readType);
					long costTime = status.getCostTime();
					if(costTime<=TimeUnit.SECONDS.toNanos(1L)){
						record.addRealTps();
					}
					record.addTimeOut(costTime);
				}else{
					
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			opsdone++;
			throttleNanos(startTimeNanos);
		}
	}
	private  Status execQueryByReadType(DBBase dbBase,Integer readType) throws Exception{
		Status status=null;
		TsPoint point=new TsPoint();
		Integer internal=24*60;//一天
		String deviceCode = Core.getDeviceCodeByRandom();
		String sensorCode = Core.getSensorCodeByRandom();
		point.setDeviceCode(deviceCode);
		point.setSensorCode(sensorCode);
		if(ReadTypeEnum.SINGLE_READ_1.getId().equals(readType)){
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_2.getId().equals(readType)){
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			FunctionParam param = Core.getFunctionBySensor(sensorCode);
			status = dbBase.selectByDeviceAndSensor(point,param.getMax(),param.getMin(), new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_3.getId().equals(readType)){
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_4.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectDayMaxByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_5.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectDayMinByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_6.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectDayAvgByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_7.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectHourMaxByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_8.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectHourMinByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_9.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectHourAvgByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_10.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMinuteMaxByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_11.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMinuteMinByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_12.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMinuteAvgByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_MAX_READ.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMaxByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_MIN_READ.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMinByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_AVG_READ.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectAvgByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_COUNT_READ.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectCountByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		return status;
	}
	public ClientThread(ReadRecord record,Integer readType, DBBase db, int opcount,
			double targetOpsPerMs) {
		this.record = record;
		this.readType=readType;//TODO 混合和优化
		this.db = db;
		this.opcount = opcount;
		this.targetOpsPerMs = targetOpsPerMs;
		this.opsdone = 0;
		if (targetOpsPerMs > 0) {
			this.targetOpsPerMs = targetOpsPerMs;
			this.targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
		}
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
		while (System.nanoTime() < deadline) {
			if (!spinSleep) {
				LockSupport.parkNanos(deadline - System.nanoTime());
			}
		}
	}
}
