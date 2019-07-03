package de.intranda.goobi.plugins;

import java.util.Date;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;

import lombok.extern.log4j.Log4j;

// This class goes to GUI folder because it ends with *QuartzListener.class
// it must be in GUI/ or lib/ folder

@WebListener
@Log4j
public class QuartzListener implements ServletContextListener {

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        // stop the Googlebooks Harvester job
        try {
            SchedulerFactory schedFact = new StdSchedulerFactory();
            Scheduler sched = schedFact.getScheduler();
            sched.deleteJob("Googlebooks Harvester", "Goobi Admin Plugin");
            log.info("Scheduler for 'Googlebooks Harvester' stopped");
        } catch (SchedulerException e) {
            log.error("Error while stopping the job", e);
        }
    }

    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        log.info("Starting 'Googlebooks Harvester' scheduler");
        try {
            // get default scheduler
            SchedulerFactory schedFact = new StdSchedulerFactory();
            Scheduler sched = schedFact.getScheduler();

            // configure time to start 
            java.util.Calendar startTime = java.util.Calendar.getInstance();
            startTime.set(java.util.Calendar.HOUR_OF_DAY, 22);
            startTime.set(java.util.Calendar.MINUTE, 0);
            startTime.set(java.util.Calendar.SECOND, 0);
            startTime.set(java.util.Calendar.MILLISECOND, 0);

            //if the startTime will be before the current time, move to next day
            if (startTime.getTime().before(new Date())) {
                startTime.add(java.util.Calendar.DAY_OF_MONTH, 1);
            }

            // create new job 
            JobDetail jobDetail = new JobDetail("Googlebooks Harvester", "Goobi Admin Plugin", QuartzJob.class);
            Trigger trigger = TriggerUtils.makeHourlyTrigger(24);
            trigger.setName("Googlebooks Harvester");
            trigger.setStartTime(startTime.getTime());

            // register job and trigger at scheduler
            sched.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            log.error("Error while executing the scheduler", e);
        }

    }

}
