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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.bytestreme.migrator.Constants.RETRY_COOLDOWN_MILLIS;
import static com.bytestreme.migrator.Constants.URL_OLD_STORAGE;

public class CoordinatorImpl implements Coordinator {

    private final static Logger logger = Logger.getLogger(CoordinatorImpl.class);

    private final int workersNumber;
    private boolean initDone = false;
    private Queue<String> files;

    public CoordinatorImpl(int workersNumber) {
        if (workersNumber < 1) throw new IllegalArgumentException("There should be at least one worker!");
        this.workersNumber = workersNumber;
    }

    @Override
    public void migrate() {
        if (!initDone) throw new IllegalStateException("Cannot migrate before init is complete!");
        final ExecutorService executorService = Executors.newFixedThreadPool(workersNumber);

        for (int i = 0; i < workersNumber; i++) {
            executorService.submit(new WorkerImpl(files));
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
        }
        logger.info("Finished.");

    }

    @Override
    public void init() {
        List<String> entities;
        do {
            entities = fetchFileList();
            if (entities == null) {
                logger.info("Could not fetch filenames. Retrying in " + (RETRY_COOLDOWN_MILLIS / 1000) + " seconds...");
                try {
                    Thread.sleep(RETRY_COOLDOWN_MILLIS);
                } catch (InterruptedException e) {
                    logger.error("Interrupted");
                }
            }
        } while (entities == null);
        logger.info("File list fetch complete. " + entities.size() + " entities.");

        files = new ConcurrentLinkedQueue<>(entities);

        initDone = true;
        logger.info("Init complete.");
    }

    private List<String> fetchFileList() {
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
