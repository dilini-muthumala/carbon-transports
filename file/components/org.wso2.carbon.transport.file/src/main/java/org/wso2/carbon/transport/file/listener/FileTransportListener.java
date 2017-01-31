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
import org.wso2.carbon.messaging.TransportListener;
import org.wso2.carbon.transport.file.common.exception.InvalidConfigurationException;
import org.wso2.carbon.transport.file.listener.config.FileTransportParams;
import org.wso2.carbon.transport.file.listener.util.Constants;

import java.util.Properties;

import static org.wso2.carbon.transport.file.common.config.PollingTransportParams.POLLING_INTERVAL;
import static org.wso2.carbon.transport.file.listener.util.Constants.TRANSPORT_FILE_FILE_URI;

public class FileTransportListener extends TransportListener {
    private static final Logger log = LoggerFactory.getLogger(FileTransportListener.class);

    private CarbonMessageProcessor messageProcessor;
    private FilePollingTask filePollingTask;


    public FileTransportListener(String id) {
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
    public boolean bind(String s) {
        return false;
    }

    @Override
    public boolean unBind(String s) {
        return false;
    }

    @Override
    public String getProtocol() {
        return Constants.PROTOCOL_NAME;
    }

    @Override
    protected void start() {
        //nothing to do
    }

    @Override
    protected void stop() {
        filePollingTask.destroy();
    }

    @Override
    protected void beginMaintenance() {
        //nothing to do
    }

    @Override
    protected void endMaintenance() {
        //nothing to do
    }

    public void startPolling() {
        Properties fileProperties = new Properties();
        fileProperties.put(TRANSPORT_FILE_FILE_URI, "file:///home/dilini/Desktop/myfile.txt");//// TODO: 1/30/17 these  params will be read from paramers, coming from Bal side.
        fileProperties.put(POLLING_INTERVAL, "1");

        FileTransportParams fileTransportParams = new FileTransportParams();
        fileTransportParams.setName("myFileReader");
        fileTransportParams.setProperties(fileProperties);

        try {
            filePollingTask = new FilePollingTask(fileTransportParams, messageProcessor);
        } catch (InvalidConfigurationException e) {
            log.error("Invalid configuration provided. Failed to create " + Constants.PROTOCOL_NAME
                    + " transport listener.", e);
        }
        filePollingTask.init();
    }
}
