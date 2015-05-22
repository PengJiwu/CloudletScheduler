package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.core.CloudSim;


public class VmCloudletAssignerFair extends VmCloudletAssigner {

	public VmCloudletAssignerFair() {
		
	}

	@Override
	public void cloudletAssign(List<QCloudlet> cloudletList, List<Vm> vmList) {
		if (vmList != null || vmList.size() != 0 || cloudletList != null
				|| cloudletList.size() != 0) {
			double sumittedTime = CloudSim.clock();
			for (QCloudlet cloudlet : cloudletList)
				cloudlet.setSubmittedTime(sumittedTime);

			List<QCloudlet> toAssignCloudletList = new ArrayList<QCloudlet>();
			if (getGlobalCloudletWaitingQueue() != null
					&& getGlobalCloudletWaitingQueue().size() != 0)
				toAssignCloudletList.addAll(getGlobalCloudletWaitingQueue());
			toAssignCloudletList.addAll(cloudletList);

			int m = vmList.size();
			int n = toAssignCloudletList.size();
			int maxCloudletsWaitingLength = ((QCloudletSchedulerSpaceShared) vmList
					.get(0).getCloudletScheduler())
					.getCloudletWaitingQueueLength();
			List<Map<String, Integer>> vmWaitingQueueSizeList = new ArrayList<Map<String, Integer>>();

			Map<String, Integer> queueSize;
			for (int i = 0; i < m; i++) {
				queueSize = new HashMap<String, Integer>();
				queueSize.put("index", i);
				queueSize.put("size", ((QCloudletSchedulerSpaceShared) vmList
						.get(i).getCloudletScheduler())
						.getCloudletWaitingQueue().size());
				vmWaitingQueueSizeList.add(queueSize);
			}
			
			for (int i = 0; i < n; i++) {
				int index = 0;
				int mSize = maxCloudletsWaitingLength + 1;
				for (int j = 0; j < m; j++) {
					if (mSize > vmWaitingQueueSizeList.get(j).get("size")) {
						mSize = vmWaitingQueueSizeList.get(j).get("size");
						index = j;
					}
				}
				if (mSize < maxCloudletsWaitingLength) {
					vmWaitingQueueSizeList.get(index).put("size", mSize++);
					toAssignCloudletList.get(i).setVmId(
							vmList.get(
									vmWaitingQueueSizeList.get(index).get("index"))
									.getId());
				}
			}
			
			getGlobalCloudletWaitingQueue().clear();
			for (int i = 0; i < n; i++) {
				if (toAssignCloudletList.get(i).getVmId() == -1) {
					getGlobalCloudletWaitingQueue().offer(
							toAssignCloudletList.get(i));
				}
			}
		}

	}

}
