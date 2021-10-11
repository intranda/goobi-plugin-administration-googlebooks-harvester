package de.intranda.goobi.plugins;

import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.interfaces.IAdministrationPlugin;
import org.goobi.production.plugin.interfaces.IPlugin;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import lombok.Data;
import lombok.extern.log4j.Log4j;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
@Data
@Log4j
public class GoogleBooksHarvesterPlugin implements IAdministrationPlugin, IPlugin {

    private static final String PLUGIN_NAME = "intranda_administration_googlebooks_harvester";
    private static final String GUI = "/uii/administration_googlebooksHarvester.xhtml";

    /**
     * Constructor for parameter initialisation from config file
     */
    public GoogleBooksHarvesterPlugin() {

    }

    @Override
    public PluginType getType() {
        return PluginType.Administration;
    }

    @Override
    public String getTitle() {
        return PLUGIN_NAME;
    }

    @Override
    public String getGui() {
        return GUI;
    }

    public void updateStatusInformation() {
        try {
            // get all job groups
            SchedulerFactory schedFact = new StdSchedulerFactory();
            Scheduler sched = schedFact.getScheduler();
            for (String groupName : sched.getJobGroupNames()) {
                // get all jobs within the group
                for (Object name : sched.getJobNames(groupName)) {
                    log.debug("Scheduler job: " + groupName + " - " + name);
                }
            }
        } catch (SchedulerException e) {
            log.error("Error while reading job information", e);
        }
    }

}
