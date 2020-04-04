package com.bytestreme.migrator.service.impl;

import com.bytestreme.migrator.service.Coordinator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.bytestreme.migrator.Constants.*;

public class CoordinatorImpl implements Coordinator {

    private final static Logger logger = Logger.getLogger(CoordinatorImpl.class);

    private final int workersNumber;
    private boolean initDone = false;
    private Queue<String> files;
    private Queue<String> backupList;

    public CoordinatorImpl(int workersNumber) {
        if (workersNumber < 1) throw new IllegalArgumentException("There should be at least one worker!");
        this.workersNumber = workersNumber;
    }

    private void invokeThreads(Queue<String> fileList) {
        final ExecutorService executorService = Executors.newFixedThreadPool(workersNumber);

        for (int i = 0; i < workersNumber; i++) {
            executorService.submit(new WorkerImpl(fileList));
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) ;
    }

    @Override
    public void migrate() {
        if (!initDone) throw new IllegalStateException("Cannot migrate before init is complete!");
        invokeThreads(files);
        logger.info("Finished main task.");
        while (true) {
            logger.info("Now, checking whether all files transmitted.");
            logger.info("Getting all transmitted files list.");
            Queue<String> transmitted = fetchUntilSucceed(URL_NEW_STORAGE);
            logger.info("Now, processing files to check by names.");
            Queue<String> absentFiles = backupList.parallelStream()
                    .filter(x -> !transmitted.contains(x))
                    .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
            if (absentFiles.isEmpty()) {
                logger.info("No absent files found.");
                logger.info("Migration successfully finished!");
                break;
            } else {
                logger.info("Found some absent files: " + absentFiles.size());
                invokeThreads(absentFiles);
            }
        }
    }

    private Queue<String> fetchUntilSucceed(String url) {
        List<String> fileNames;
        do {
            fileNames = fetchStorageFileList(url);
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

        return new ConcurrentLinkedQueue<>(fileNames);
    }


    @Override
    public CoordinatorImpl init() {
        files = fetchUntilSucceed(URL_OLD_STORAGE);
        backupList = new LinkedList<>(files);

        initDone = true;
        logger.info("Init complete.");
        return this;
    }

    private List<String> fetchStorageFileList(String url) {
        try (CloseableHttpClient httpClient = HttpClients.createMinimal()) {
            HttpGet fileListFetchRequest = new HttpGet(url);
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
