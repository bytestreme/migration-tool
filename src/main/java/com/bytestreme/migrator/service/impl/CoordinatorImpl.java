package com.bytestreme.migrator.service.impl;

import com.bytestreme.migrator.service.Coordinator;
import com.bytestreme.migrator.struct.MigrationConfig;
import com.bytestreme.migrator.struct.WorkerMode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static com.bytestreme.migrator.Constants.RETRY_COOLDOWN_MILLIS;
import static com.bytestreme.migrator.Constants.URL_OLD_STORAGE;

public class CoordinatorImpl implements Coordinator {

    private final static Logger logger = Logger.getLogger(CoordinatorImpl.class);

    private boolean initDone = false;
    private Queue<String> transmitFileList;
    private Queue<String> deleteFileList;
    private final MigrationConfig config;

    public CoordinatorImpl(MigrationConfig config) {
        this.config = config;
        if (config.getWorkersNumber() < 1) throw new IllegalArgumentException("There should be at least one worker!");
    }

    private void invokeThreads(Queue<String> fileList, WorkerMode mode) {
        final ExecutorService executorService = Executors.newFixedThreadPool(config.getWorkersNumber());

        for (int i = 0; i < config.getWorkersNumber(); i++) {
            executorService.submit(mode == WorkerMode.TRANSMIT
                    ? new Transmitter(fileList)
                    : new Cleaner(fileList));
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) ;
    }

    @Override
    public void migrate() {
        if (!initDone) throw new IllegalStateException("Cannot migrate before init is complete!");
        if (transmitFileList.isEmpty()) {
            logger.info("No files to transmit. Exiting...");
            return;
        }
        invokeThreads(transmitFileList, WorkerMode.TRANSMIT);
        logger.info("Finished main task.");
        logger.info("Now, cleaning files from the old storage...");
        invokeThreads(new LinkedBlockingQueue<>(deleteFileList), WorkerMode.CLEAN);
        logger.info("Finished cleaning.");
        logger.info("Migration successfully finished!");
    }

    private Queue<String> fetchUntilSucceed() {
        List<String> fileNames;
        do {
            fileNames = fetchStorageFileList();
            if (fileNames == null) {
                logger.info("Could not fetch filenames. Retrying in " + (RETRY_COOLDOWN_MILLIS / 1000) + " seconds...");
                try {
                    Thread.sleep(RETRY_COOLDOWN_MILLIS);
                } catch (InterruptedException e) {
                    logger.error("Interrupted");
                }
            }
        } while (fileNames == null);
        logger.info("File list fetch complete. " + fileNames.size() + " files.");

        return new LinkedBlockingQueue<>(fileNames);
    }


    @Override
    public CoordinatorImpl init() {
        transmitFileList = fetchUntilSucceed();
        deleteFileList = new LinkedBlockingQueue<>(transmitFileList);

        initDone = true;
        logger.info("Init complete.");
        return this;
    }

    private List<String> fetchStorageFileList() {
        try (CloseableHttpClient httpClient = HttpClients.createMinimal()) {
            HttpGet fileListFetchRequest = new HttpGet(URL_OLD_STORAGE);
            CloseableHttpResponse response = httpClient.execute(fileListFetchRequest);
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode != 200) return null;

            return Arrays.asList(new ObjectMapper().readValue(response.getEntity().getContent(), String[].class));

        } catch (IOException e) {
            logger.error(e);
            return null;
        }
    }

}
