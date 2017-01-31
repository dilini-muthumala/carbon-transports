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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;

/**
 * This class executes a task at a given interval.
 */
public abstract class PeriodicTask {

    private static final Log logger = LogFactory.getLog(PeriodicTask.class.getName());

    /**
     * When creating a new task, a name is required to be provided,
     * so a task can be identified uniquely.
     */
    private String name;

    public static final int TASK_THRESHOLD_INTERVAL = 1000;

    protected long interval;

    public PeriodicTask(String name, long interval) {
        this.name = name;
        this.interval = interval;
    }

    public String getName() {
        return name;
    }

    public void execute() {
        logger.debug("Common task: " + name + " executing.");

        //If the thresehold value is greater than i second just run the cycle
        if (interval >= TASK_THRESHOLD_INTERVAL) {
            taskExecute();
        } else {
            long lStartTime = (new Date()).getTime();
            long lCurrentTime = lStartTime;
            //Run the cycles within one second (1000ms)
            while ((lCurrentTime - lStartTime) < TASK_THRESHOLD_INTERVAL) {
                taskExecute();
                long lEndTime = (new Date()).getTime();
                long lRequiredSleep = interval - (lEndTime - lCurrentTime);
                if (lRequiredSleep > 0) {
                    try {
                        Thread.sleep(lRequiredSleep);
                    } catch (InterruptedException e) {
                        logger.debug("Unable to sleep the common task: " + name + " thread less than 1 second");
                    }
                }
                lCurrentTime = (new Date()).getTime();
            }
        }
    }

    protected abstract void taskExecute();
}

