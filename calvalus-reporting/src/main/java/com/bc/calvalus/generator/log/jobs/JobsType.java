
package com.bc.calvalus.generator.log.jobs;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author muhammad.bc.
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "jobs")
@XmlType(name = "jobsType", propOrder = {
        "job"
})
public class JobsType {

    protected List<JobType> job;

    /**
     * Gets the value of the job.xml property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the job.xml property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getJob().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link JobType }
     * 
     * 
     */
    public List<JobType> getJob() {
        if (job == null) {
            job = new ArrayList<JobType>();
        }
        return this.job;
    }

}
