package org.cloudbus.cloudsim;

import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

public class GlobalBroker extends SimEntity {

	private static final int CREATE_BROKER = 0;
	private List<Vm> vmList;
	private List<QCloudlet> cloudletList;
	
	private static List<QDatacenterBroker> brokerList;
	private static List<Double> brokerDelayList;
	private static List<Integer> numLetList;
	
	public GlobalBroker(String name) {
		super(name);
	}

	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		case CREATE_BROKER:
			int brokerId = ((Integer) ev.getData()).intValue();
			brokerList.add(createBroker("Broker_" + brokerId));

			//Create VMs and Cloudlets and send them to broker
			setVmList(createVM(brokerList.get(brokerId).getId(), 10, 100));
			setCloudletList(createCloudlet(brokerList.get(brokerId).getId(), 
					numLetList.get(brokerId), brokerId * 1000 + 1000));

			brokerList.get(brokerId).submitVmList(getVmList());
			brokerList.get(brokerId).submitCloudletList(getCloudletList());

			CloudSim.resumeSimulation();

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
		brokerList = new LinkedList<QDatacenterBroker>();
		for (int i = 0; i < brokerDelayList.size(); i++) {
			schedule(getId(), brokerDelayList.get(i), CREATE_BROKER, i);
		}
	}

	@Override
	public void shutdownEntity() {
	}

	public List<Vm> getVmList() {
		return vmList;
	}

	protected void setVmList(List<Vm> vmList) {
		this.vmList = vmList;
	}

	public List<QCloudlet> getCloudletList() {
		return cloudletList;
	}

	protected void setCloudletList(List<QCloudlet> cloudletList) {
		this.cloudletList = cloudletList;
	}
	
	//////////////////////////////////////        My methods below       ///////////////////////////////////////////
	
	protected List<QDatacenterBroker> getBrokerList() {
		return brokerList;
	}
	
	private static QDatacenterBroker createBroker(String name){

		QDatacenterBroker broker = null;
		try {
			broker = new QDatacenterBroker(name);
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
		int mips = 250;
		long bw = 1000;
		int pesNumber = 1; //number of cpus
		String vmm = "Xen"; //VMM name

		Vm[] vm = new Vm[vms];

		for(int i=0;i<vms;i++){
			vm[i] = new Vm(idShift + i, userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
			list.add(vm[i]);
		}

		return list;
	}
	
	private static List<QCloudlet> createCloudlet(int userId, int cloudlets, int idShift){
		LinkedList<QCloudlet> list = new LinkedList<QCloudlet>();

		long length = 40000;
		long fileSize = 300;
		long outputSize = 300;
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
	
	private static long f_Unif(long a, long b) {
		long result;
		
		result = (long) (a + Math.random() * (b - a + 1));
		
		return result;
	}

	public static void createCloudletWave(int numLetWave, double lambda) throws Exception {//生成globalBroker序列
		int numLet[] = new int[numLetWave];
		
		for (int i = 0; i < numLetWave; i++) {
			numLet[i] = (int) (1000 * f_Poisson(lambda, 10));
			if (numLet[i] <= 0) {
				numLet[i] = 1;
			}
			System.out.println("numLet[" + i + "]: " + numLet[i] + "\tlambda: " + lambda + "\tf_Poisson: " + f_Poisson(lambda, 10));
			lambda += 0.2;
		}
		
		brokerDelayList = new LinkedList<Double>();
		numLetList = new LinkedList<Integer>();
		for (int i = 0; i < numLetWave; i++) {
			brokerDelayList.add(i, 200.0 * i);
			numLetList.add(i, numLet[i]);
		}
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

}
