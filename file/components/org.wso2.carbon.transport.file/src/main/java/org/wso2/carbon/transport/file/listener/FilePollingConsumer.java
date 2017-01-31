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

import org.apache.commons.lang.WordUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileNotFoundException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.UriParser;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.transport.file.common.FileLockingParamsDTO;
import org.wso2.carbon.transport.file.listener.exception.FileTransportException;
import org.wso2.carbon.transport.file.listener.util.Constants;
import org.wso2.carbon.transport.file.listener.util.FileTransportUtils;
import org.wso2.carbon.transport.file.message.FileCarbonMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.lang.Math.toIntExact;

/**
 * Polls for a file and consumes it.
 */
public class FilePollingConsumer {

    private static final Log log = LogFactory.getLog(FilePollingConsumer.class);
    private Properties vfsProperties;
    private boolean fileLock = true;
    private FileSystemManager fsManager = null;
    private String name;
    private long scanInterval;
    private Long lastRanTime;
    private int lastCycle;

    private CarbonMessageProcessor messageProcessor;

    private String fileURI;
    private FileObject fileObject;
    private Integer iFileProcessingInterval = null;
    private Integer iFileProcessingCount = null;
    private int maxRetryCount;
    private long reconnectionTimeout;
    private String strFilePattern;
    private boolean autoLockRelease;
    private Boolean autoLockReleaseSameNode;
    private Long autoLockReleaseInterval;
    private FileSystemOptions fso;

    public FilePollingConsumer(Properties vfsProperties, String name, long scanInterval, CarbonMessageProcessor messageProcessor) {
        this.vfsProperties = vfsProperties;
        this.name = name;
        this.scanInterval = scanInterval;
        this.lastRanTime = null;
        this.messageProcessor = messageProcessor;

        setupParams();
        try {
            StandardFileSystemManager fsm = new StandardFileSystemManager();
            fsm.setConfiguration(getClass().getClassLoader().getResource("providers.xml"));
            fsm.init();
            fsManager = fsm;
        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        //Setup SFTP Options
        try {
            fso = FileTransportUtils.attachFileSystemOptions(parseSchemeFileOptions(fileURI), fsManager);
        } catch (Exception e) {
            log.warn("Unable to set the sftp options", e);
            fso = null;
        }
    }

    /**
     * This will be called by the task scheduler. If a cycle execution takes
     * more than the schedule interval, tasks will call this method ignoring the
     * interval. Timestamp based check is done to avoid that.
     */
    public void execute() {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Start : File-polling for : " + name);
            }
            // Check if the cycles are running in correct interval and start
            // scan
            long currentTime = (new Date()).getTime();
            if (lastRanTime == null || ((lastRanTime + (scanInterval)) <= currentTime)) {
                lastRanTime = currentTime;
                poll();
            } else if (log.isDebugEnabled()) {
                log.debug("Skip file-polling cycle since current rate is higher than the scan interval, for : "
                        + name);
            }
            if (log.isDebugEnabled()) {
                log.debug("End : File-polling for : " + name);
            }
        } catch (Exception e) {
            log.error("Error while reading file. " + e.getMessage(), e);
        }
    }

    /**
     * Do the file processing operation for the given set of properties. Do the
     * checks and pass the control to processFile method
     */
    public FileObject poll() {
        if (fileURI == null || fileURI.trim().equals("")) {
            log.error("Invalid file url. Check the File transport configuration. File transport instance name : "
                    + name + ", File URL : " + FileTransportUtils.maskURLPassword(fileURI));
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Start : Scanning directory or file : " + FileTransportUtils.maskURLPassword(fileURI));
        }

        if (!initFileCheck()) {
            // Unable to read from the source location provided.
            return null;
        }

        // If file/folder found proceed to the processing stage
        try {
            lastCycle = 0;
            if (fileObject.exists() && fileObject.isReadable()) {
                FileObject[] children = null;
                try {
                    children = fileObject.getChildren();
                } catch (FileNotFolderException ignored) {
                    if (log.isDebugEnabled()) {
                        log.debug("No Folder found. Only file found on : "
                                + FileTransportUtils.maskURLPassword(fileURI));
                    }
                } catch (FileSystemException ex) {
                    log.error(ex.getMessage(), ex);
                }

                // if this is a file that would translate to a single message
                if (children == null || children.length == 0) {
                    // Fail record is a one that is processed but was not moved
                    // or deleted due to an error.
                    boolean isFailedRecord = FileTransportUtils.isFailRecord(fsManager, fileObject);
                    if (!isFailedRecord) {
                        fileHandler();
                    } else {
                        try {
                            lastCycle = 2;
                            moveOrDeleteAfterProcessing(fileObject);
                        } catch (FileTransportException transportException) {
                            log.error("File object '" + FileTransportUtils.maskURLPassword(fileObject.getURL().toString()) + "' "
                                    + "cloud not be moved after first attempt", transportException);
                        }
                        if (fileLock) {
                            // TODO: passing null to avoid build break. Fix properly
                            FileTransportUtils.releaseLock(fsManager, fileObject, fso);
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("File '" + FileTransportUtils.maskURLPassword(fileObject.getURL().toString())
                                    + "' has been marked as a failed"
                                    + " record, it will not process");
                        }
                    }
                } else {
                    FileObject fileObject = directoryHandler(children);
                    if (fileObject != null) {
                        return fileObject;
                    }
                }
            } else {
                log.warn("Unable to access or read file or directory : "
                        + FileTransportUtils.maskURLPassword(fileURI)
                        + "."
                        + " Reason: "
                        + (fileObject.exists() ? (fileObject.isReadable() ? "Unknown reason"
                        : "The file can not be read!") : "The file does not exists!"));
                return null;
            }
        } catch (FileSystemException e) {
            log.error(
                    "Error checking for existence and readability : "
                            + FileTransportUtils.maskURLPassword(fileURI), e);
            return null;
        } catch (Exception e) {
            log.error(
                    "Error while processing the file/folder in URL : "
                            + FileTransportUtils.maskURLPassword(fileURI), e);
            return null;
        } finally {
            try {
                if (fsManager != null) {
                    fsManager.closeFileSystem(fileObject.getParent().getFileSystem());
                }
                fileObject.close();
            } catch (Exception e) {
                log.error("Unable to close the file system. " + e.getMessage());
                log.error(e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("End : Scanning directory or file : " + FileTransportUtils.maskURLPassword(fileURI));
        }
        return null;
    }

    /**
     * If not a folder just a file handle the flow
     *
     * @throws FileSystemException
     */
    private void fileHandler() throws FileSystemException {
        if (fileObject.getType() == FileType.FILE) {
            if (!fileLock || (fileLock && acquireLock(fsManager, fileObject))) {
                boolean runPostProcess = true;
                try {
                    if (processFile(fileObject) == null) {
                        runPostProcess = false;
                    }
                    lastCycle = 1;
                } catch (FileTransportException e) {
                    lastCycle = 2;
                    log.error("Error processing File URI : "
                            + FileTransportUtils.maskURLPassword(fileObject.getName().toString()), e);
                }

                if (runPostProcess) {
                    try {
                        moveOrDeleteAfterProcessing(fileObject);
                    } catch (FileTransportException synapseException) {
                        lastCycle = 3;
                        log.error("File object '" + FileTransportUtils.maskURLPassword(fileObject.getURL().toString()) + "' "
                                + "cloud not be moved", synapseException);
                        FileTransportUtils.markFailRecord(fsManager, fileObject);
                    }
                }

                if (fileLock) {
                    // TODO: passing null to avoid build break. Fix properly
                    FileTransportUtils.releaseLock(fsManager, fileObject, fso);
                    if (log.isDebugEnabled()) {
                        log.debug("Removed the lock file '" + FileTransportUtils.maskURLPassword(fileObject.toString())
                                + ".lock' of the file '" + FileTransportUtils.maskURLPassword(fileObject.toString()));
                    }
                }

            } else {
                log.error("Couldn't get the lock for processing the file : " +
                        FileTransportUtils.maskURLPassword(fileObject.getName().toString()));
            }

        } else {
            if (log.isDebugEnabled()) {
                log.debug("Cannot find the file or failed file record. File : "
                        + FileTransportUtils.maskURLPassword(fileURI));
            }
        }
    }

    /**
     * Setup the required transport parameters
     */
    private void setupParams() {

        fileURI = vfsProperties.getProperty(Constants.TRANSPORT_FILE_FILE_URI);

        String strFileLock = vfsProperties.getProperty(Constants.TRANSPORT_FILE_LOCKING);
        if (strFileLock != null
                && strFileLock.toLowerCase().equals(Constants.TRANSPORT_FILE_LOCKING_DISABLED)) {
            fileLock = false;
        }

        strFilePattern = vfsProperties.getProperty(Constants.TRANSPORT_FILE_FILE_NAME_PATTERN);
        if (vfsProperties.getProperty(Constants.TRANSPORT_FILE_INTERVAL) != null) {
            try {
                iFileProcessingInterval = Integer.valueOf(vfsProperties
                        .getProperty(Constants.TRANSPORT_FILE_INTERVAL));
            } catch (NumberFormatException e) {
                log.warn("Invalid param value for transport.vfs.FileProcessInterval : "
                        + vfsProperties.getProperty(Constants.TRANSPORT_FILE_INTERVAL)
                        + ". Expected numeric value.");
            }
        }
        if (vfsProperties.getProperty(Constants.TRANSPORT_FILE_COUNT) != null) {
            try {
                iFileProcessingCount = Integer.valueOf(vfsProperties
                        .getProperty(Constants.TRANSPORT_FILE_COUNT));
            } catch (NumberFormatException e) {
                log.warn("Invalid param value for transport.vfs.FileProcessCount : "
                        + vfsProperties.getProperty(Constants.TRANSPORT_FILE_COUNT)
                        + ". Expected numeric value.");
            }
        }
        maxRetryCount = 0;
        if (vfsProperties.getProperty(Constants.MAX_RETRY_COUNT) != null) {
            try {
                maxRetryCount = Integer.valueOf(vfsProperties
                        .getProperty(Constants.MAX_RETRY_COUNT));
            } catch (NumberFormatException e) {
                log.warn("Invalid values for Max Retry Count");
                maxRetryCount = 0;
            }
        }

        reconnectionTimeout = 1;
        if (vfsProperties.getProperty(Constants.RECONNECT_TIMEOUT) != null) {
            try {
                reconnectionTimeout = Long.valueOf(vfsProperties
                        .getProperty(Constants.RECONNECT_TIMEOUT));
            } catch (NumberFormatException e) {
                log.warn("Invalid values for Reconnection Timeout");
                reconnectionTimeout = 1;
            }
        }

        String strAutoLock = vfsProperties.getProperty(Constants.TRANSPORT_AUTO_LOCK_RELEASE);
        autoLockRelease = false;
        autoLockReleaseSameNode = true;
        autoLockReleaseInterval = null;
        if (strAutoLock != null) {
            try {
                autoLockRelease = Boolean.parseBoolean(strAutoLock);
            } catch (Exception e) {
                autoLockRelease = false;
                log.warn("VFS Auto lock removal not set properly. Current value is : "
                        + strAutoLock, e);
            }
            if (autoLockRelease) {
                String strAutoLockInterval = vfsProperties
                        .getProperty(Constants.TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL);
                if (strAutoLockInterval != null) {
                    try {
                        autoLockReleaseInterval = Long.parseLong(strAutoLockInterval);
                    } catch (Exception e) {
                        autoLockReleaseInterval = null;
                        log.warn(
                                "VFS Auto lock removal property not set properly. Current value is : "
                                        + strAutoLockInterval, e);
                    }
                }
                String strAutoLockReleaseSameNode = vfsProperties
                        .getProperty(Constants.TRANSPORT_AUTO_LOCK_RELEASE_SAME_NODE);
                if (strAutoLockReleaseSameNode != null) {
                    try {
                        autoLockReleaseSameNode = Boolean.parseBoolean(strAutoLockReleaseSameNode);
                    } catch (Exception e) {
                        autoLockReleaseSameNode = true;
                        log.warn(
                                "VFS Auto lock removal property not set properly. Current value is : "
                                        + autoLockReleaseSameNode, e);
                    }
                }
            }

        }
    }

    private Map<String, String> parseSchemeFileOptions(String fileURI) {
        String scheme = UriParser.extractScheme(fileURI);
        if (scheme == null) {
            return null;
        }
        HashMap<String, String> schemeFileOptions = new HashMap<String, String>();
        schemeFileOptions.put(Constants.SCHEME, scheme);

        try {
            addOptions(scheme, schemeFileOptions);
        } catch (Exception e) {
            log.warn("Error while loading VFS parameter. " + e.getMessage());
        }
        return schemeFileOptions;
    }

    private void addOptions(String scheme, Map<String, String> schemeFileOptions) {
        if (scheme.equals(Constants.SCHEME_SFTP)) {
            for (Constants.SFTP_FILE_OPTION option : Constants.SFTP_FILE_OPTION.values()) {
                String strValue = vfsProperties.getProperty(Constants.SFTP_PREFIX
                        + WordUtils.capitalize(option.toString()));
                if (strValue != null && !strValue.equals("")) {
                    schemeFileOptions.put(option.toString(), strValue);
                }
            }
        }
    }

    /**
     * Handle directory with chile elements
     *
     * @param children
     * @return
     * @throws FileSystemException
     */
    private FileObject directoryHandler(FileObject[] children) throws FileSystemException {
        // Process Directory
        lastCycle = 0;
        int failCount = 0;
        int successCount = 0;
        int processCount = 0;

        if (log.isDebugEnabled()) {
            log.debug("File name pattern : "
                    + vfsProperties.getProperty(Constants.TRANSPORT_FILE_FILE_NAME_PATTERN));
        }

        // Sort the files
        String strSortParam = vfsProperties.getProperty(Constants.FILE_SORT_PARAM);
        if (strSortParam != null && !"NONE".equals(strSortParam)) {
            log.debug("Start Sorting the files.");
            String strSortOrder = vfsProperties.getProperty(Constants.FILE_SORT_ORDER);
            boolean bSortOrderAsscending = true;
            if (strSortOrder != null && strSortOrder.toLowerCase().equals("false")) {
                bSortOrderAsscending = false;
            }
            if (log.isDebugEnabled()) {
                log.debug("Sorting the files by : " + strSortOrder + ". (" + bSortOrderAsscending
                        + ")");
            }
            if (strSortParam.equals(Constants.FILE_SORT_VALUE_NAME) && bSortOrderAsscending) {
                Arrays.sort(children, new FileNameAscComparator());
            } else if (strSortParam.equals(Constants.FILE_SORT_VALUE_NAME)
                    && !bSortOrderAsscending) {
                Arrays.sort(children, new FileNameDesComparator());
            } else if (strSortParam.equals(Constants.FILE_SORT_VALUE_SIZE)
                    && bSortOrderAsscending) {
                Arrays.sort(children, new FileSizeAscComparator());
            } else if (strSortParam.equals(Constants.FILE_SORT_VALUE_SIZE)
                    && !bSortOrderAsscending) {
                Arrays.sort(children, new FileSizeDesComparator());
            } else if (strSortParam.equals(Constants.FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP)
                    && bSortOrderAsscending) {
                Arrays.sort(children, new FileLastmodifiedtimestampAscComparator());
            } else if (strSortParam.equals(Constants.FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP)
                    && !bSortOrderAsscending) {
                Arrays.sort(children, new FileLastmodifiedtimestampDesComparator());
            }
            log.debug("End Sorting the files.");
        }

        for (FileObject child : children) {
            // skipping *.lock / *.fail file
            if (child.getName().getBaseName().endsWith(".lock")
                    || child.getName().getBaseName().endsWith(".fail")) {
                continue;
            }
            boolean isFailedRecord = FileTransportUtils.isFailRecord(fsManager, child);

            // child's file name matches the file name pattern or process all
            // files now we try to get the lock and process
            if ((strFilePattern == null || child.getName().getBaseName().matches(strFilePattern))
                    && !isFailedRecord) {

                if (log.isDebugEnabled()) {
                    log.debug("Matching file : " + child.getName().getBaseName());
                }

                if ((!fileLock || (fileLock && acquireLock(fsManager, child)))) {
                    // process the file
                    boolean runPostProcess = true;
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("Processing file :" + FileTransportUtils.maskURLPassword(child.toString()));
                        }
                        processCount++;
                        if (processFile(child) == null) {
                            runPostProcess = false;
                        } else {
                            successCount++;
                        }
                        // tell moveOrDeleteAfterProcessing() file was success
                        lastCycle = 1;
                    } catch (Exception e) {
                        if (e.getCause() instanceof FileNotFoundException) {
                            log.warn("Error processing File URI : " +
                                    FileTransportUtils.maskURLPassword(child.getName().toString()) +
                                    ". This can be due to file moved from another process.");
                            runPostProcess = false;
                        } else {
                            log.error("Error processing File URI : " +
                                    FileTransportUtils.maskURLPassword(child.getName().toString()), e);
                            failCount++;
                            // tell moveOrDeleteAfterProcessing() file failed
                            lastCycle = 2;
                        }

                    }
                    // skipping un-locking file if failed to do delete/move
                    // after process
                    boolean skipUnlock = false;
                    if (runPostProcess) {
                        try {
                            moveOrDeleteAfterProcessing(child);
                        } catch (FileTransportException fileTransportException) {
                            log.error("File object '" + FileTransportUtils.maskURLPassword(child.getURL().toString())
                                            + "'cloud not be moved, will remain in \"locked\" state",
                                    fileTransportException);
                            skipUnlock = true;
                            failCount++;
                            lastCycle = 3;
                            FileTransportUtils.markFailRecord(fsManager, child);
                        }
                    }
                    // if there is a failure or not we'll try to release the
                    // lock
                    if (fileLock && !skipUnlock) {
                        // TODO: passing null to avoid build break. Fix properly
                        FileTransportUtils.releaseLock(fsManager, child, fso);
                    }
                }
            } else if (log.isDebugEnabled() && strFilePattern != null
                    && !child.getName().getBaseName().matches(strFilePattern) && !isFailedRecord) {
                // child's file name does not match the file name pattern
                log.debug("Non-Matching file : " + child.getName().getBaseName());
            } else if (isFailedRecord) {
                // it is a failed record
                try {
                    lastCycle = 1;
                    moveOrDeleteAfterProcessing(child);
                } catch (FileTransportException fileTransportException) {
                    log.error("File object '" + FileTransportUtils.maskURLPassword(child.getURL().toString())
                            + "'cloud not be moved, will remain in \"fail\" state", fileTransportException);
                }
                if (fileLock) {
                    // TODO: passing null to avoid build break. Fix properly
                    FileTransportUtils.releaseLock(fsManager, child, fso);
                    FileTransportUtils.releaseLock(fsManager, fileObject, fso);
                }
                if (log.isDebugEnabled()) {
                    log.debug("File '" + FileTransportUtils.maskURLPassword(fileObject.getURL().toString())
                            + "' has been marked as a failed record, it will not " + "process");
                }
            }

            //close the file system after processing
            try {
                child.close();
            } catch (Exception e) {
            }

            // Manage throttling of file processing
            if (iFileProcessingInterval != null && iFileProcessingInterval > 0) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Put the VFS processor to sleep for : " + iFileProcessingInterval);
                    }
                    Thread.sleep(iFileProcessingInterval);
                } catch (InterruptedException ie) {
                    log.error("Unable to set the interval between file processors." + ie);
                }
            } else if (iFileProcessingCount != null && iFileProcessingCount <= processCount) {
                break;
            }
        }
        if (failCount == 0 && successCount > 0) {
            lastCycle = 1;
        } else if (successCount == 0 && failCount > 0) {
            lastCycle = 4;
        } else {
            lastCycle = 5;
        }
        return null;
    }

    /**
     * Check if the file/folder exists before proceeding and retrying
     */
    private boolean initFileCheck() {
        boolean wasError = true;
        int retryCount = 0;

        fileObject = null;
        while (wasError) {
            try {
                retryCount++;
                fileObject = fsManager.resolveFile(fileURI, fso);
                if (fileObject == null) {
                    log.error("fileObject is null");
                    throw new FileSystemException("fileObject is null");
                }
                wasError = false;
            } catch (FileSystemException e) {
                if (retryCount >= maxRetryCount) {
                    log.error(
                            "Repeatedly failed to resolve the file URI: "
                                    + FileTransportUtils.maskURLPassword(fileURI), e);
                    return false;
                } else {
                    log.warn("Failed to resolve the file URI: " + FileTransportUtils.maskURLPassword(fileURI)
                            + ", in attempt " + retryCount + ", " + e.getMessage()
                            + " Retrying in " + reconnectionTimeout + " milliseconds.");
                }
            }
            if (wasError) {
                try {
                    Thread.sleep(reconnectionTimeout);
                } catch (InterruptedException e2) {
                    log.error("Thread was interrupted while waiting to reconnect.", e2);
                }
            }
        }
        return true;
    }

    /**
     * Do the file level locking
     *
     * @param fsManager
     * @param fileObject
     * @return
     */
    private boolean acquireLock(FileSystemManager fsManager, FileObject fileObject) {
        String strContext = fileObject.getName().getURI();
        boolean rtnValue;
        // When processing a directory list is fetched initially. Therefore
        // there is still a chance of file processed by another process.
        // Need to check the source file before processing.
        try {
            String parentURI = fileObject.getParent().getName().getURI();
            if (parentURI.contains("?")) {
                String suffix = parentURI.substring(parentURI.indexOf("?"));
                strContext += suffix;
            }
            FileObject sourceFile = fsManager.resolveFile(strContext);
            if (!sourceFile.exists()) {
                return false;
            }
        } catch (FileSystemException e) {
            return false;
        }
        FileLockingParamsDTO lockingParamsDTO = new FileLockingParamsDTO();
        lockingParamsDTO.setAutoLockRelease(autoLockRelease);
        lockingParamsDTO.setAutoLockReleaseSameNode(autoLockReleaseSameNode);
        lockingParamsDTO.setAutoLockReleaseInterval(autoLockReleaseInterval);
        rtnValue = FileTransportUtils.acquireLock(fsManager, fileObject, lockingParamsDTO, fso, true);
        return rtnValue;
    }

    /**
     * Actual processing of the file/folder
     *
     * @param file
     * @return
     */
    private FileObject processFile(FileObject file) {
        FileContent content;
        try {
            content = file.getContent();
            String fileName = file.getName().getBaseName();
            String filePath = file.getName().getPath();
            String fileURI = file.getName().getURI();

//            if (injectHandler != null) {
            Map<String, Object> transportHeaders = new HashMap<>();
            transportHeaders.put(Constants.FILE_PATH, filePath);
            transportHeaders.put(Constants.FILE_NAME, fileName);
            transportHeaders.put(Constants.FILE_URI, fileURI);

            try {
                transportHeaders.put(Constants.FILE_LENGTH, content.getSize());
                transportHeaders.put(Constants.LAST_MODIFIED, content.getLastModifiedTime());
            } catch (FileSystemException e) {
                log.warn("Unable to set file length or last modified date header.", e);
            }

            CarbonMessage cMessage = new FileCarbonMessage();
            cMessage.setProperty("TRANSPORT_HEADERS", transportHeaders);    // TODO: 1/31/17 find the const to replace TRANSPORT_HEADERS
            byte[] byteArr = new byte[toIntExact(content.getSize())];
            content.getOutputStream().write(byteArr);

            ByteBuffer byteBuffer = ByteBuffer.wrap(byteArr);
            cMessage.addMessageBody(byteBuffer);
            // TODO: 1/30/17 IMPORTANT there's a classcast exception here.

            messageProcessor.receive(cMessage, null);
        } catch (FileSystemException e) {
            log.error("Error reading file content or attributes : " + FileTransportUtils.maskURLPassword(file.toString()), e);
        } catch (IOException e) {
            // TODO: 1/27/17
        } catch (Exception e) {
            // TODO: 1/30/17
        }
        return file;
    }

    /**
     * Do the post processing actions
     *
     * @param fileObject
     */
    private void moveOrDeleteAfterProcessing(FileObject fileObject) {

        String moveToDirectoryURI = null;
        try {
            switch (lastCycle) {
                case 1:
                    if (Constants.AFTER_ACTION_MOVE.equals(vfsProperties
                            .getProperty(Constants.TRANSPORT_FILE_ACTION_AFTER_PROCESS))) {
                        moveToDirectoryURI = vfsProperties
                                .getProperty(Constants.TRANSPORT_FILE_MOVE_AFTER_PROCESS);
                        //Postfix the date given timestamp format
                        String strSubfoldertimestamp = vfsProperties
                                .getProperty(Constants.SUBFOLDER_TIMESTAMP);
                        if (strSubfoldertimestamp != null) {
                            try {
                                SimpleDateFormat sdf = new SimpleDateFormat(strSubfoldertimestamp);
                                String strDateformat = sdf.format(new Date());
                                int iIndex = moveToDirectoryURI.indexOf("?");
                                if (iIndex > -1) {
                                    moveToDirectoryURI = moveToDirectoryURI.substring(0, iIndex)
                                            + strDateformat
                                            + moveToDirectoryURI.substring(iIndex,
                                            moveToDirectoryURI.length());
                                } else {
                                    moveToDirectoryURI += strDateformat;
                                }
                            } catch (Exception e) {
                                log.warn("Error generating subfolder name with date", e);
                            }
                        }
                    }
                    break;

                case 2:
                    if ("MOVE".equals(vfsProperties
                            .getProperty(Constants.TRANSPORT_FILE_ACTION_AFTER_FAILURE))) {
                        moveToDirectoryURI = vfsProperties
                                .getProperty(Constants.TRANSPORT_FILE_MOVE_AFTER_FAILURE);
                    }
                    break;

                default:
                    return;
            }

            if (moveToDirectoryURI != null) {
                FileObject moveToDirectory = fsManager.resolveFile(moveToDirectoryURI, fso);
                String prefix;
                if (vfsProperties.getProperty(Constants.TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT) != null) {
                    prefix = new SimpleDateFormat(
                            vfsProperties
                                    .getProperty(Constants.TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT))
                            .format(new Date());
                } else {
                    prefix = "";
                }

                //Forcefully create the folder(s) if does not exists
                String strForceCreateFolder = vfsProperties.getProperty(Constants.FORCE_CREATE_FOLDER);
                if (strForceCreateFolder != null && strForceCreateFolder.toLowerCase().equals("true") && !moveToDirectory.exists()) {
                    moveToDirectory.createFolder();
                }

                FileObject dest = moveToDirectory.resolveFile(prefix
                        + fileObject.getName().getBaseName());
                if (log.isDebugEnabled()) {
                    log.debug("Moving to file :" + FileTransportUtils.maskURLPassword(dest.getName().getURI()));
                }
                try {
                    fileObject.moveTo(dest);
                    if (FileTransportUtils.isFailRecord(fsManager, fileObject)) {
                        FileTransportUtils.releaseFail(fsManager, fileObject);
                    }
                } catch (FileSystemException e) {
                    if (!FileTransportUtils.isFailRecord(fsManager, fileObject)) {
                        FileTransportUtils.markFailRecord(fsManager, fileObject);
                    }
                    log.error("Error moving file : " + FileTransportUtils.maskURLPassword(fileObject.toString()) + " to " +
                            FileTransportUtils.maskURLPassword(moveToDirectoryURI), e);
                }
            } else {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Deleting file :" + FileTransportUtils.maskURLPassword(fileObject.toString()));
                    }
                    fileObject.close();
                    if (!fileObject.delete()) {
                        String msg = "Cannot delete file : " + FileTransportUtils.maskURLPassword(fileObject.toString());
                        log.error(msg);
                        throw new FileTransportException(msg);
                    }
                } catch (FileSystemException e) {
                    log.error("Error deleting file : " + FileTransportUtils.maskURLPassword(fileObject.toString()), e);
                }
            }
        } catch (FileSystemException e) {
            if (!FileTransportUtils.isFailRecord(fsManager, fileObject)) {
                FileTransportUtils.markFailRecord(fsManager, fileObject);
                log.error("Error resolving directory to move after processing : "
                        + FileTransportUtils.maskURLPassword(moveToDirectoryURI), e);
            }
        }
    }

    /**
     * Comparator classed used to sort the files according to user input
     */
    class FileNameAscComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            return o1.getName().compareTo(o2.getName());
        }
    }

    class FileLastmodifiedtimestampAscComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            Long lDiff = 0l;
            try {
                lDiff = o1.getContent().getLastModifiedTime()
                        - o2.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                log.warn("Unable to compare lastmodified timestamp of the two files.", e);
            }
            return lDiff.intValue();
        }
    }

    class FileSizeAscComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            Long lDiff = 0l;
            try {
                lDiff = o1.getContent().getSize() - o2.getContent().getSize();
            } catch (FileSystemException e) {
                log.warn("Unable to compare size of the two files.", e);
            }
            return lDiff.intValue();
        }
    }

    class FileNameDesComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            return o2.getName().compareTo(o1.getName());
        }
    }

    class FileLastmodifiedtimestampDesComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            Long lDiff = 0l;
            try {
                lDiff = o2.getContent().getLastModifiedTime()
                        - o1.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                log.warn("Unable to compare lastmodified timestamp of the two files.", e);
            }
            return lDiff.intValue();
        }
    }

    class FileSizeDesComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            Long lDiff = 0l;
            try {
                lDiff = o2.getContent().getSize() - o1.getContent().getSize();
            } catch (FileSystemException e) {
                log.warn("Unable to compare size of the two files.", e);
            }
            return lDiff.intValue();
        }
    }

}
