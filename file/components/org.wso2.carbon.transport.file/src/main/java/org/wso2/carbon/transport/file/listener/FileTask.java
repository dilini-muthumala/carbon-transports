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

/**
 * FileTask class will execute the file-polling consumer
 */
public class FileTask extends PeriodicTask {

    private static final Log logger = LogFactory.getLog(FileTask.class.getName());

    private FilePollingConsumer pollingConsumer;

    public FileTask(FilePollingConsumer consumer, String name, long interval) {
        super(name, interval);
        this.pollingConsumer = consumer;
        logger.debug("File-polling task: " + name + " initialized with interval: " + interval);
    }

    protected void taskExecute() {
        logger.debug("File-polling task: " + getName() + " executing...");
//        pollingConsumer.execute();
    }
}
