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
	public void cloudletAssign(List<QCloudlet> cloudletList, List<Vm> vmList) {
		if (vmList != null || vmList.size() != 0 || cloudletList != null
				|| cloudletList.size() != 0) {
			double sumittedTime = CloudSim.clock();
			for (QCloudlet cloudlet : cloudletList)
				cloudlet.setSubmittedTime(sumittedTime);

			int m = vmList.size();
			int n = cloudletList.size();
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

			int randnVmIndex[] = new int[n];
			for (int i = 0; i < n; i++)
				randnVmIndex[i] = randomInt(0, m);

			for (int i = 0; i < n; i++) {
				int mSize = vmWaitingQueueSizeList.get(randnVmIndex[i]).get(
						"size");
				if (mSize <= maxCloudletsWaitingLength) {
					vmWaitingQueueSizeList.get(randnVmIndex[i]).put("size",
							mSize++);
					cloudletList.get(i).setVmId(
							vmList.get(randnVmIndex[i]).getId());
					randnVmIndex[i] = -1; // 已经使用
				}

			}			

			// 还有没有分配VM的的任务
			for (int i = 0; i < n; i++) {
				if (cloudletList.get(i).getVmId() != -1) {
					// 根据等待队列大小对vmlist排序,从小到大
					Collections.sort(vmWaitingQueueSizeList,
							new Comparator<Map<String, Integer>>() {
								public int compare(Map<String, Integer> queue1,
										Map<String, Integer> queue2) {
									return (queue1.get("size").compareTo(queue2
											.get("size")));
								}
							});
					//等待队列最空的
					int mSize = vmWaitingQueueSizeList.get(0).get("size");
					if (mSize < maxCloudletsWaitingLength) {
						cloudletList.get(i).setVmId(
								vmList.get(
										vmWaitingQueueSizeList.get(0).get(
												"index")).getId());
						vmWaitingQueueSizeList.get(i).put("size", mSize++);
					} else	//全都满了
						break;
				}
			}

			// 所有Vm的CloudletWaitingQueue都满了
			for (int i = 0; i < n; i++) {
				if (cloudletList.get(i).getVmId() != -1) {
					getGlobalCloudletWaitingQueue().offer(cloudletList.get(i));
				}
			}

		}

	}

	private int randomInt(int min, int max) { // random[min,max] 可取min,可取max
		Random random = new Random();
		return random.nextInt(max) % (max - min + 1) + min;
	}

}
