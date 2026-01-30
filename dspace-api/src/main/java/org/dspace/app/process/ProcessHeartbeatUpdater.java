package org.dspace.app.process;

import org.apache.logging.log4j.Logger;
import org.dspace.core.Context;
import org.dspace.scripts.factory.ScriptServiceFactory;
import org.dspace.scripts.service.ProcessService;

/**
 * Helper class for updating the heartbeat of the currently running instance processes.
 */
public class ProcessHeartbeatUpdater {
    private static final Logger log = org.apache.logging.log4j.LogManager.getLogger(ProcessHeartbeatUpdater.class);

    private static final ProcessService processService = ScriptServiceFactory.getInstance().getProcessService();

    public static void updateProcessesHeartbeats() {
        try {
            Context context = new Context();
            context.turnOffAuthorisationSystem();
            processService.updateProcessesHeartbeat(context);
            context.commit();
        } catch (Exception e) {
            log.error("Error when trying to update processes heartbeats", e);
        }
    }
}
