/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.transport.file.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.transport.file.common.PeriodicTask;

import java.util.Date;

/**
 * The {@link Runnable} which runs a periodic task.
 */
public class PollingTaskRunner implements Runnable {

    private static final Log log = LogFactory.getLog(PollingTaskRunner.class);

    private volatile boolean execute = true;

    private PeriodicTask task;
    private long lastRuntime;
    private long currentRuntime;
    private long cycleInterval;
    private long interval;

    public PollingTaskRunner(PeriodicTask task, long interval) {
        this.task = task;
        this.interval = interval;
    }

    @Override
    public void run() {
        log.debug("Starting the polling task.");
        // Wait for the clustering configuration to be loaded.

        log.debug("Configuration context loaded. Running the Inbound Endpoint.");
        // Run the poll cycles
        while (execute) {
            log.debug("Executing the polling task.");
            lastRuntime = getTime();
            try {
                task.execute();
            } catch (Exception e) {
                log.error("Error executing the inbound endpoint polling cycle.", e);
            }
            currentRuntime = getTime();
            cycleInterval = interval - (currentRuntime - lastRuntime);
            if (cycleInterval > 0) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Unable to sleep the polling task thread for interval of : " + interval + "ms.");
                    }
                }
            }
        }
        log.debug("Exit the polling task running loop.");
    }

    /**
     * Exit the running while loop and terminate the thread
     */
    protected void terminate() {
        execute = false;
    }

    private Long getTime() {
        return new Date().getTime();
    }
}
