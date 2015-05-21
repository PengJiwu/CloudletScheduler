package org.cloudbus.cloudsim;

public class QCloudlet extends Cloudlet {

	private double submittedTime;	//提交到CloudletAssigner的时间
	
	public QCloudlet(int cloudletId, long cloudletLength, int pesNumber,
			long cloudletFileSize, long cloudletOutputSize,
			UtilizationModel utilizationModelCpu,
			UtilizationModel utilizationModelRam,
			UtilizationModel utilizationModelBw) {
		super(cloudletId, cloudletLength, pesNumber, cloudletFileSize,
				cloudletOutputSize, utilizationModelCpu, utilizationModelRam,
				utilizationModelBw);
		// TODO Auto-generated constructor stub
	}

	@Override
	public double getWaitingTime() {
		return getExecStartTime() - getSubmittedTime();
	}
	
	public double getSubmittedTime() {
		return submittedTime;
	}

	public void setSubmittedTime(double submittedTime) {
		this.submittedTime = submittedTime;
	}

	

}
