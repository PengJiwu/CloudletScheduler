package org.cloudbus.cloudsim;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;

public class QDatacenterBroker extends DatacenterBroker {

	private VmCloudletAssigner vmCloudletAssigner;
	
	public QDatacenterBroker(String name) throws Exception {
		super(name);
	}

	@Override
	protected void submitCloudlets() {
		getVmCloudletAssigner().cloudletAssign(getCloudletList(),getVmList());
		for (Cloudlet cloudlet : getCloudletList()) {
			Vm vm;
			if (cloudlet.getVmId() != -1) {	
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) { // vm was not created
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
							+ cloudlet.getCloudletId() + ": bount VM not available");
					continue;
				}
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet "
					+ cloudlet.getCloudletId() + " to VM #" + vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			getCloudletSubmittedList().add(cloudlet);
			}
		}

		// remove submitted cloudlets from waiting list
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}

	public VmCloudletAssigner getVmCloudletAssigner() {
		return vmCloudletAssigner;
	}

	public void setVmCloudletAssigner(VmCloudletAssigner vmCloudletAssigner) {
		this.vmCloudletAssigner = vmCloudletAssigner;
	}			
	
}
