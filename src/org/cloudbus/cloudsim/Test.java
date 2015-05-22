package org.cloudbus.cloudsim;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class Test {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;

	/** The vmList. */
	private static List<Vm> vmList;
	
	/**
	 * Creates main() to run this example
	 */
	public static void main(String[] args) {
		Log.printLine("Starting...");

		try {
			int num_user = 2;
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;

			CloudSim.init(num_user, calendar, trace_flag);

			@SuppressWarnings("unused")
			int numHost = 3;
			Datacenter datacenter0 = createDatacenter("Datacenter_0", numHost);

			QDatacenterBroker qDatacenterBroker = new QDatacenterBroker("QDatacenterBroker");

			CloudSim.startSimulation();

			List<Cloudlet> newList = new LinkedList<Cloudlet>();
			for (int i = 0; i < qDatacenterBroker.getBrokerList().size(); i++) {
				newList.addAll(qDatacenterBroker.getBrokerList().get(i).getCloudletReceivedList());
			}

			CloudSim.stopSimulation();

			printCloudletList(newList);

			Log.printLine("finished!");
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static Datacenter createDatacenter(String name, int numHost){

		List<Host> hostList = new ArrayList<Host>();

		List<Pe> peList1 = new ArrayList<Pe>();

		int mips = 10000;

		peList1.add(new Pe(0, new PeProvisionerSimple(mips)));
//		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));

//		List<Pe> peList2 = new ArrayList<Pe>();
//		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));

		int hostId=0;
		int ram = 16384;
		long storage = 1000000;
		int bw = 10000;
		
		Host sampleHost = new Host(
				hostId,
				new RamProvisionerSimple(ram),
				new BwProvisionerSimple(bw),
				storage,
				peList1,
				new VmSchedulerTimeShared(peList1)
			);

		for (int i = 0; i < numHost; i++) {
			hostList.add(sampleHost);
	
			hostId++;
		}

		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.1;	// the cost of using storage in this resource
		double costPerBw = 0.1;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
				"Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");

				Log.printLine( indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId() +
						indent + indent + indent + dft.format(cloudlet.getActualCPUTime()) +
						indent + indent + dft.format(cloudlet.getExecStartTime())+ indent + indent + indent + dft.format(cloudlet.getFinishTime()));
			}
		}

	}

}
