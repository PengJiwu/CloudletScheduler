package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

public class QDatacenterBroker extends DatacenterBroker {

	private VmCloudletAssigner vmCloudletAssigner;
	
	private static final int CREATE_CLOUDLETS = 49;
	private static final int CLOUDLETS_SUBMIT = 50;
	
//	private static List<DatacenterBroker> brokerList;
	private static List<Double> delayList;
	private static List<Integer> numLetList;
	private int currWave;
	
	public QDatacenterBroker(String name,VmCloudletAssigner vmCloudletAssigner) throws Exception {
		super(name);
		setCloudletList(new ArrayList<QCloudlet>());
		setCloudletSubmittedList(new ArrayList<QCloudlet>());
		setCloudletReceivedList(new ArrayList<QCloudlet>());
		setVmCloudletAssigner(vmCloudletAssigner);
		
		currWave = 0;
	}
	
	@Override
	public void processOtherEvent(SimEvent ev) {
		switch (ev.getTag()) {
		case CREATE_CLOUDLETS:
			int waveId = ((Integer) ev.getData()).intValue();
//			brokerList.add(createBroker("Broker_" + brokerId));
			System.out.println("第" + (waveId + 1) + "波cloudlet开始到达");

			//Create Cloudlets and send them to broker
			submitCloudletList(createCloudlet(getId(), 
					numLetList.get(waveId), waveId * 1000 + 1000));
			currWave++;
			
			if (waveId > 0) {
				sendNow(getId(), CLOUDLETS_SUBMIT);
			}

			CloudSim.resumeSimulation();

			break;
		case CLOUDLETS_SUBMIT:
			submitCloudlets();
			break;
		default:
			Log.printLine(getName() + ": unknown event type");
			break;
		}
	}
	

	@Override
	public void startEntity() {
		Log.printLine(super.getName()+" is starting...");
		try {
			createCloudletWave(100, 0);
		} catch (Exception e) {
			System.out.println("生成云任务队列出错！");
			e.printStackTrace();
		}
		setVmList(createVM(getId(), 10, 0));
//		brokerList = new ArrayList<DatacenterBroker>();
		for (int i = 0; i < delayList.size(); i++) {
			schedule(getId(), delayList.get(i), CREATE_CLOUDLETS, i);
		}
		schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
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

	@Override
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
			
			if (currWave < delayList.size()) {
				System.out.println("后面还有n波任务没有到达。。。。");
				return;
			}
			
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

			
	
	////////////////////////////////             from globalBroker         ////////////////////////////////
	
//	protected List<DatacenterBroker> getBrokerList() {
//		return brokerList;
//	}
	
	private static DatacenterBroker createBroker(String name){

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker(name);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	private static List<Vm> createVM(int userId, int vms, int idShift) {
		LinkedList<Vm> list = new LinkedList<Vm>();

		long size = 10000; //image size (MB)
		int ram = 512; //vm memory (MB)
		int mips = 10000;//250;
		long bw = 1000;
		int pesNumber = 1; //number of cpus
		String vmm = "Xen"; //VMM name
		int cloudletWaitingQueueLength = 50;

		Vm[] vm = new Vm[vms];

		for(int i=0;i<vms;i++){
			vm[i] = new Vm(idShift + i, userId, mips, pesNumber, ram, bw, size, vmm, 
					new QCloudletSchedulerSpaceShared(cloudletWaitingQueueLength));
			list.add(vm[i]);
		}

		return list;
	}
	
	private static List<QCloudlet> createCloudlet(int userId, int cloudlets, int idShift){
		LinkedList<QCloudlet> list = new LinkedList<QCloudlet>();

		long length = 40000;
		long fileSize = 0;
		long outputSize = 0;
		int pesNumber = 1;
		UtilizationModel utilizationModel = new UtilizationModelFull();

		QCloudlet[] cloudlet = new QCloudlet[cloudlets];

		for(int i=0;i<cloudlets;i++){
			cloudlet[i] = new QCloudlet(idShift + i, length, pesNumber, fileSize, outputSize, 
					utilizationModel, utilizationModel, utilizationModel);
			cloudlet[i].setUserId(userId);
			list.add(cloudlet[i]);
		}

		return list;
	}
	
	public static double f_Poisson(double lambda, int k) {//泊松分布
		double e = 2.7182818284;		
		double result;
		
		result = Math.pow(e, -lambda) * Math.pow(lambda, k);
		for (int i = 1; i <= k; i++) {
			result = result / i;
		}
		
		return result;
	}

	public static void createCloudletWave(int numLetWave, double lambda) throws Exception {//生成Broker的延迟序列delayList
		int numLet[] = new int[numLetWave];
		
		for (int i = 0; i < numLetWave; i++) {
			numLet[i] = (int) (1000 * f_Poisson(lambda, 10));
			if (numLet[i] <= 0) {
				numLet[i] = 1;
			}
			System.out.println("numLet[" + i + "]: " + numLet[i] + "\tlambda: " + lambda + "\tf_Poisson: " + f_Poisson(lambda, 10));
			lambda += 0.2;
		}
		
		delayList = new LinkedList<Double>();
		numLetList = new LinkedList<Integer>();
		for (int i = 0; i < numLetWave; i++) {
			delayList.add(i, 200.0 * i);
			numLetList.add(i, numLet[i]);
		}
	}	
	


}
