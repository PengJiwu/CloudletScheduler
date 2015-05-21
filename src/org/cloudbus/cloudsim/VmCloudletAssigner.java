package org.cloudbus.cloudsim;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public abstract class VmCloudletAssigner {

	protected static Queue<QCloudlet> globalCloudletWaitingQueue = new LinkedList<QCloudlet>();

	public abstract void cloudletAssign(List<QCloudlet> cloudletList,
			List<Vm> vmList);

	public static Queue<QCloudlet> getGlobalCloudletWaitingQueue() {
		return globalCloudletWaitingQueue;
	}

	public static void setGlobalCloudletWaitingQueue(
			Queue<QCloudlet> globalCloudletWaitingQueue) {
		VmCloudletAssigner.globalCloudletWaitingQueue = globalCloudletWaitingQueue;
	}

}
