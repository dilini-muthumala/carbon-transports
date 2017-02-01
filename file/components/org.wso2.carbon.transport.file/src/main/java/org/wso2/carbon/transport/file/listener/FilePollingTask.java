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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.transport.file.common.PeriodicTask;
import org.wso2.carbon.transport.file.common.PollingTaskScheduler;
import org.wso2.carbon.transport.file.common.config.PollingTransportParams;
import org.wso2.carbon.transport.file.common.exception.InvalidConfigurationException;
import org.wso2.carbon.transport.file.listener.config.FileTransportParams;

import java.util.Properties;

/**
 * Polls for a file or a folder periodically, at a given location.
 * Once a file is seen, the content of the file is read and it will be sent to the message processor.
 */
public class FilePollingTask extends PollingTaskScheduler {
    private static final Logger log = LoggerFactory.getLogger(FilePollingTask.class);

    private FilePollingConsumer filePollingConsumer;
    private Properties vfsProperties;
    private CarbonMessageProcessor messageProcessor;

    public FilePollingTask(FileTransportParams params, CarbonMessageProcessor messageProcessor) throws InvalidConfigurationException {
        this.name = params.getName();
        this.vfsProperties = params.getProperties();
        this.messageProcessor = messageProcessor;
        try {
            this.interval = Long.parseLong(vfsProperties
                    .getProperty(PollingTransportParams.POLLING_INTERVAL));
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Invalid number format provided for property '" +
                    PollingTransportParams.POLLING_INTERVAL + "'",e);
        }
    }

    /**
     * This will have to be called at the time of service creation.
     */
    public void init() {
        log.info("Inbound file listener " + name + " starting ...");
        filePollingConsumer = new FilePollingConsumer(vfsProperties, name, interval, messageProcessor);
        start();
    }

    /**
     * Register/start the schedule service
     */
    public void start() {
        PeriodicTask task = new FileTask(filePollingConsumer, name, interval);
        startTask(task);
    }
}
