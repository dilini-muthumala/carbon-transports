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
import org.wso2.carbon.messaging.PollingServerConnector;

/**
 * Server connector for File transport.
 */
public class FileServerConnector extends PollingServerConnector {
    private static final Logger log = LoggerFactory.getLogger(FileServerConnector.class);

    private CarbonMessageProcessor messageProcessor;
    private FilePollingTask filePollingTask;

    public FileServerConnector(String id) {
        super(id);
    }

    @Override
    public void setMessageProcessor(CarbonMessageProcessor carbonMessageProcessor) {
        messageProcessor = carbonMessageProcessor;
    }

    public CarbonMessageProcessor getMessageProcessor() {
        return messageProcessor;
    }

    @Override
    protected void start() {
        //nothing to do
    }

    @Override
    public void stop() {
        if (filePollingTask != null) {
            filePollingTask.destroy();
        }
    }

    @Override
    protected void beginMaintenance() {
        //nothing to do
    }

    @Override
    protected void endMaintenance() {
        //nothing to do
    }
//
//    public void poll(Map<String, String> map) {
//        FileTransportParams fileTransportParams = new FileTransportParams();
//        fileTransportParams.setName(getId());
//        fileTransportParams.setProperties(map);
//
//        try {
//            filePollingTask = new FilePollingTask(fileTransportParams, messageProcessor);
//        } catch (InvalidConfigurationException e) {
//            log.error("Invalid configuration provided. Failed to create " + Constants.PROTOCOL_NAME
//                    + " transport listener.", e);
//        }
//        filePollingTask.init();
//    }

    @Override
    public void poll() {
        FilePollingConsumer consumer = new FilePollingConsumer(getParameters());
        consumer.poll();
    }
}
