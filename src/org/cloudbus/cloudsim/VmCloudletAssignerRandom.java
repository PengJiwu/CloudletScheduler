package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudbus.cloudsim.core.CloudSim;

public class VmCloudletAssignerRandom extends VmCloudletAssigner {

	@Override
	public boolean cloudletAssign(List<QCloudlet> cloudletList, List<Vm> vmList) {
		if (vmList != null || vmList.size() != 0) {
			List<QCloudlet> toAssignCloudletList = new ArrayList<QCloudlet>();
			if (cloudletList != null && cloudletList.size() != 0) {
				double sumittedTime = CloudSim.clock();
				for (QCloudlet cloudlet : cloudletList)
					cloudlet.setSubmittedTime(sumittedTime);	//设置到达时间
				
				if (getGlobalCloudletWaitingQueue().size() != 0) {
					toAssignCloudletList
							.addAll(getGlobalCloudletWaitingQueue());
					getGlobalCloudletWaitingQueue().clear();
				}
				toAssignCloudletList.addAll(cloudletList);	//添加提交的任务为待分配任务
				
			} else {// cloudletList为null 从主队列中分配一个任务
				if (getGlobalCloudletWaitingQueue().size() != 0)
					toAssignCloudletList.add(getGlobalCloudletWaitingQueue()
							.poll());
				else
					return false;
			}

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
				int index = randomInt(0, m);
				int mSize = vmWaitingQueueSizeList.get(index).get("size");
				if (mSize >= maxCloudletsWaitingLength) {// 若随机的队列满了，往最空的队列
					// 根据等待队列大小对vmlist排序,从小到大
					Collections.sort(vmWaitingQueueSizeList,
							new Comparator<Map<String, Integer>>() {
								public int compare(Map<String, Integer> queue1,
										Map<String, Integer> queue2) {
									return (queue1.get("size").compareTo(queue2
											.get("size")));
								}
							});
					index = 0;
					mSize = vmWaitingQueueSizeList.get(0).get("size");
					if (mSize >= maxCloudletsWaitingLength)
						break;
				}

				vmWaitingQueueSizeList.get(index).put("size", mSize++);
				toAssignCloudletList.get(i).setVmId(
						vmList.get(
								vmWaitingQueueSizeList.get(index).get("index"))
								.getId());

			}

			/*
			 * // 还有没有分配VM的的任务 for (int i = 0; i < n; i++) { if
			 * (toAssignCloudletList.get(i).getVmId() != -1) { //
			 * 根据等待队列大小对vmlist排序,从小到大 Collections.sort(vmWaitingQueueSizeList,
			 * new Comparator<Map<String, Integer>>() { public int
			 * compare(Map<String, Integer> queue1, Map<String, Integer> queue2)
			 * { return (queue1.get("size").compareTo(queue2 .get("size"))); }
			 * }); // 等待队列最空的 int mSize =
			 * vmWaitingQueueSizeList.get(0).get("size"); if (mSize <
			 * maxCloudletsWaitingLength) { toAssignCloudletList.get(i).setVmId(
			 * vmList.get( vmWaitingQueueSizeList.get(0).get(
			 * "index")).getId()); vmWaitingQueueSizeList.get(i).put("size",
			 * mSize++); } else // 全都满了 break; } }
			 */

			// 所有Vm的CloudletWaitingQueue都满了

			for (int i = 0; i < n; i++) {
				if (toAssignCloudletList.get(i).getVmId() == -1) {
					getGlobalCloudletWaitingQueue().offer(
							toAssignCloudletList.get(i));
				}
			}

		} else
			return false;

		return true;
	}

	private int randomInt(int min, int max) { // random[min,max] 可取min,可取max
		Random random = new Random();
		return random.nextInt(max) % (max - min + 1) + min;
	}

}
