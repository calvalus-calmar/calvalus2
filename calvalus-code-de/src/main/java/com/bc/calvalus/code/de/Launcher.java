package com.bc.calvalus.code.de;import com.bc.calvalus.code.de.reader.JobDetail;import com.bc.calvalus.code.de.reader.ReadJobDetail;import com.bc.calvalus.code.de.sender.FactoryProcessedMessage;import com.bc.calvalus.code.de.sender.ProcessedMessage;import com.bc.calvalus.code.de.sender.SendMessage;import com.bc.wps.utilities.PropertiesWrapper;import java.io.IOException;import java.util.List;import java.util.concurrent.Executors;import java.util.concurrent.ScheduledExecutorService;import org.apache.log4j.BasicConfigurator;import org.apache.log4j.Logger;import static java.util.concurrent.TimeUnit.SECONDS;/** * @author muhammad.bc. */public class Launcher implements Runnable {    static Logger logger = Logger.getLogger(Launcher.class);    public Launcher() {        loadProperties();        int delay = Integer.parseInt(PropertiesWrapper.get("submit.time.interval"));        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();        scheduledExecutorService.scheduleWithFixedDelay(this, 0, delay, SECONDS);    }    @Override    public void run() {        ReadJobDetail readJobDetail = new ReadJobDetail();        List<JobDetail> jobDetail = readJobDetail.getJobDetail();        if (jobDetail.size() <= 0) {            logger.info(String.format("Job details between %s is empty %s", readJobDetail.startDateTime()));            return;        }        ProcessedMessage[] processedMessage = FactoryProcessedMessage.createProcessedMessage(jobDetail);        SendMessage sendMessage = new SendMessage(processedMessage);        sendMessage.send();    }    static void loadProperties() {        try {            PropertiesWrapper.loadConfigFile("conf/calvalus-code-de.properties");            BasicConfigurator.configure();            logger.info("Load property setting....");        } catch (IOException e) {            logger.error(e.getMessage());        }    }    public static void main(String[] args) {        Launcher launcher = new Launcher();    }}