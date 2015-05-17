/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;

/**
 *
 * @author YAO
 */
public class QCloudlet extends Cloudlet {

    private double waitingTime;

    public QCloudlet(int cloudletId, long cloudletLength, int pesNumber, long cloudletFileSize, long cloudletOutputSize, UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw) {
        super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw);
    }

    /**
     * Get the value of waitingTime
     *
     * @return the value of waitingTime
     */
    public double getWaitingTime() {
        return waitingTime;
    }

    /**
     * Set the value of waitingTime
     *
     * @param waitingTime new value of waitingTime
     */
    public void setWaitingTime(double waitingTime) {
        this.waitingTime = waitingTime;
    }
}
