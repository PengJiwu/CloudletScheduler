package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.core.CloudSim;

public class QCloudletSchedulerSpaceShared extends CloudletSchedulerSpaceShared {

	private List<CloudletQueue> vmCloudletWaitingQueue;
	private List<ResCloudlet> decardedCloudletList;

	public QCloudletSchedulerSpaceShared() {
		super();
		decardedCloudletList = new ArrayList<ResCloudlet>();
	}

	@Override
	public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
		setCurrentMipsShare(mipsShare);
		/*
		 * double timeSpam = currentTime - getPreviousTime(); // time since last
		 * update double capacity = 0.0; int cpus = 0;
		 * 
		 * for (Double mips : mipsShare) { // count the CPUs available to the
		 * VMM capacity += mips; if (mips > 0) { cpus++; } } currentCpus = cpus;
		 * capacity /= cpus; // average capacity of each cpu
		 * 
		 * // each machine in the exec list has the same amount of cpu for
		 * (ResCloudlet rcl : getCloudletExecList()) {
		 * rcl.updateCloudletFinishedSoFar((long) (capacity * timeSpam *
		 * rcl.getNumberOfPes() * Consts.MILLION)); }
		 * 
		 * // no more cloudlets in this scheduler if
		 * (getCloudletExecList().size() == 0 && getCloudletWaitingList().size()
		 * == 0) { setPreviousTime(currentTime); return 0.0; }
		 * 
		 * // update each cloudlet int finished = 0; List<ResCloudlet> toRemove
		 * = new ArrayList<ResCloudlet>(); for (ResCloudlet rcl :
		 * getCloudletExecList()) { // finished anyway, rounding issue... if
		 * (rcl.getRemainingCloudletLength() == 0) { toRemove.add(rcl);
		 * cloudletFinish(rcl); finished++; } }
		 * getCloudletExecList().removeAll(toRemove);
		 * 
		 * // for each finished cloudlet, add a new one from the waiting list if
		 * (!getCloudletWaitingList().isEmpty()) { for (int i = 0; i < finished;
		 * i++) { toRemove.clear(); for (ResCloudlet rcl :
		 * getCloudletWaitingList()) { if ((currentCpus - usedPes) >=
		 * rcl.getNumberOfPes()) { rcl.setCloudletStatus(Cloudlet.INEXEC); for
		 * (int k = 0; k < rcl.getNumberOfPes(); k++) { rcl.setMachineAndPeId(0,
		 * i); } getCloudletExecList().add(rcl); usedPes +=
		 * rcl.getNumberOfPes(); toRemove.add(rcl); break; } }
		 * getCloudletWaitingList().removeAll(toRemove); } }
		 * 
		 * // estimate finish time of cloudlets in the execution queue double
		 * nextEvent = Double.MAX_VALUE; for (ResCloudlet rcl :
		 * getCloudletExecList()) { double remainingLength =
		 * rcl.getRemainingCloudletLength(); double estimatedFinishTime =
		 * currentTime + (remainingLength / (capacity * rcl.getNumberOfPes()));
		 * if (estimatedFinishTime - currentTime <
		 * CloudSim.getMinTimeBetweenEvents()) { estimatedFinishTime =
		 * currentTime + CloudSim.getMinTimeBetweenEvents(); } if
		 * (estimatedFinishTime < nextEvent) { nextEvent = estimatedFinishTime;
		 * } } setPreviousTime(currentTime); return nextEvent;
		 */
		return 0;
	}

	public class CloudletQueue {
		private int vmId;					//虚拟机ID
		private Queue<ResCloudlet> waitingCLoudletQueue;	//任务队列
		private double averageWaitingTime;	//平均等待时间
		private int NFinishedCloudlet;		//已完成的任务数
		private int queueLength;			//队列长度

		public CloudletQueue(int vmId, int length) {
			super();
			setVmId(vmId);
			setNFinishedCloudlet(0);
			setQueueLength(length);
			waitingCLoudletQueue = new LinkedList<ResCloudlet>();
			averageWaitingTime = 0;
		}

		public int getVmId() {
			return vmId;
		}

		public void setVmId(int vmId) {
			this.vmId = vmId;
		}

		public boolean addCloudlet(ResCloudlet cloudlet) {
			if (waitingCLoudletQueue.size() < getQueueLength())
				return waitingCLoudletQueue.offer(cloudlet);
			else
				return false;
		}

		public ResCloudlet removeCloudlet() {
			return waitingCLoudletQueue.poll();
		}

		private double updateAverageWaitingTime(double newWaitingTime) {
			return (getAverageWaitingTime() * getNFinishedCloudlet() + newWaitingTime)
					/ (getNFinishedCloudlet());
		}

		public double getAverageWaitingTime() {
			return averageWaitingTime;
		}

		public void setAverageWaitingTime(double averageWaitingTime) {
			this.averageWaitingTime = averageWaitingTime;
		}

		public int getNFinishedCloudlet() {
			return NFinishedCloudlet;
		}

		public void setNFinishedCloudlet(int nFinishedCloudlet) {
			NFinishedCloudlet = nFinishedCloudlet;
		}

		public int getQueueLength() {
			return queueLength;
		}

		public void setQueueLength(int queueLength) {
			this.queueLength = queueLength;
		}

	}

	public List<ResCloudlet> getDecardedCloudletList() {
		return decardedCloudletList;
	}

	public void setDecardedCloudletList(List<ResCloudlet> decardedCloudletList) {
		this.decardedCloudletList = decardedCloudletList;
	}

}
