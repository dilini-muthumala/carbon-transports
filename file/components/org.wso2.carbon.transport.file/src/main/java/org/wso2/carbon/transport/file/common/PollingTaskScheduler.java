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

package org.wso2.carbon.transport.file.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class is responsible for starting a task,
 * as well as destroying the tasks started by itself, when required.
 */
public class PollingTaskScheduler {
    private static final Logger log = LoggerFactory.getLogger(PollingTaskScheduler.class);

    protected long interval = 1000L;    //todo: decide a suitable default polling interval at the review
    protected String name;

    private HashMap<Thread, PollingTaskRunner> inboundRunnersThreadsMap = new HashMap<>();

    /**
     * Starts a period task; also keeps track of it.
     * @param task the period task which needs to be run.
     */
    public void startTask(PeriodicTask task) {
        PollingTaskRunner inboundRunner = new PollingTaskRunner(task, interval);
        Thread runningThread = new Thread(inboundRunner);
        inboundRunnersThreadsMap.put(runningThread, inboundRunner);
        runningThread.start();

        //simple execution where polling will happen only once.
//        task.taskExecute();
    }

    /**
     * This will be called when the period tasks started by this scheduler needs to be stopped.
     * e.g. when stopping the server, call this to stop all the threads.
     */
    public void destroy() {
        log.info("Polling task for " + name + " is stopping.");
        if (!inboundRunnersThreadsMap.isEmpty()) {
            Iterator itr = inboundRunnersThreadsMap.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry entry = (Map.Entry) itr.next();
                Thread thread = (Thread) entry.getKey();
                PollingTaskRunner pollingTaskRunner = (PollingTaskRunner) entry.getValue();
                pollingTaskRunner.terminate();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    log.error("Error while stopping the polling thread for " + name);
                }
            }
            inboundRunnersThreadsMap.clear();
        }
    }
}
