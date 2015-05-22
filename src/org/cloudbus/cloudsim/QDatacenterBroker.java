package org.cloudbus.cloudsim;

import java.util.ArrayList;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

public class QDatacenterBroker extends DatacenterBroker {

	private VmCloudletAssigner vmCloudletAssigner;

	public QDatacenterBroker(String name) throws Exception {
		super(name);
		setCloudletList(new ArrayList<QCloudlet>());
		setCloudletSubmittedList(new ArrayList<QCloudlet>());
		setCloudletReceivedList(new ArrayList<QCloudlet>());
	}

	@Override
	protected void submitCloudlets() {
		// 将任务与虚拟机绑定
		getVmCloudletAssigner().cloudletAssign(
				this.<QCloudlet> getCloudletList(), getVmList());

		for (QCloudlet cloudlet : this.<QCloudlet> getCloudletList()) {
			Vm vm;
			if (cloudlet.getVmId() != -1) {
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) { // vm was not created
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": Postponing execution of cloudlet "
							+ cloudlet.getCloudletId()
							+ ": bount VM not available");
					continue;
				}
				Log.printLine(CloudSim.clock() + ": " + getName()
						+ ": Sending cloudlet " + cloudlet.getCloudletId()
						+ " to VM #" + vm.getId());
				sendNow(getVmsToDatacentersMap().get(vm.getId()),
						CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
				cloudletsSubmitted++;
				this.<QCloudlet> getCloudletSubmittedList().add(cloudlet);
			}
		}

		// remove submitted cloudlets from waiting list
		for (QCloudlet cloudlet : this.<QCloudlet> getCloudletSubmittedList()) {
			this.<QCloudlet> getCloudletList().remove(cloudlet);
		}
	}

	protected void processCloudletReturn(SimEvent ev) {
		QCloudlet cloudlet = (QCloudlet) ev.getData();
		this.<QCloudlet> getCloudletReceivedList().add(cloudlet);
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet "
				+ cloudlet.getCloudletId() + " received");
		cloudletsSubmitted--;
		// 从主队列调度一个任务
		if (vmCloudletAssigner.cloudletAssign(null, getVmList())) {
			for (QCloudlet cl : this.<QCloudlet> getCloudletList()) {
				Vm vm;
				if (cloudlet.getVmId() != -1) {
					vm = VmList.getById(getVmsCreatedList(), cl.getVmId());
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": Sending cloudlet " + cl.getCloudletId()
							+ " to VM #" + vm.getId());
					sendNow(getVmsToDatacentersMap().get(vm.getId()),
							CloudSimTags.CLOUDLET_SUBMIT, cl);
					cloudletsSubmitted++;
					getCloudletSubmittedList().add(cl);
					getCloudletList().remove(cl);
				}
				break;
			}
		}

		if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) { // all
																		// cloudlets
																		// executed
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": All Cloudlets executed. Finishing...");
			clearDatacenters();
			finishExecution();
		} else { // some cloudlets haven't finished yet
			if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
				// all the cloudlets sent finished. It means that some bount
				// cloudlet is waiting its VM be created
				clearDatacenters();
				createVmsInDatacenter(0);
			}
		}

	}

	public VmCloudletAssigner getVmCloudletAssigner() {
		return vmCloudletAssigner;
	}

	public void setVmCloudletAssigner(VmCloudletAssigner vmCloudletAssigner) {
		this.vmCloudletAssigner = vmCloudletAssigner;
	}

}
