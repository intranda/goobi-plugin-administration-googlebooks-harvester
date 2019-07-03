package de.intranda.goobi.plugins;

import org.quartz.Job;
import org.quartz.JobExecutionContext;

import lombok.extern.log4j.Log4j;

@Log4j
public class QuartzJob implements Job {

    @Override
    public void execute(JobExecutionContext context) {
        log.debug("Execute job: " + context.getJobDetail().getName() + " - " + context.getRefireCount());

        int numberHarvested = 0;

        log.debug(String.format("Googlebooks harvester: %d processes were created.", numberHarvested));

    }

}
