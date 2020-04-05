package com.bytestreme.migrator.service.impl;

import com.bytestreme.migrator.service.Worker;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Queue;

import static com.bytestreme.migrator.Constants.URL_OLD_STORAGE;

public class Cleaner implements Worker {

    private final static Logger logger = Logger.getLogger(Cleaner.class);

    private Queue<String> fileNames;

    public Cleaner(Queue<String> fileNames) {
        this.fileNames = fileNames;
    }

    private int cleanFromOldStorage(String fileName) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createMinimal();
        HttpDelete oldStorageRequest = new HttpDelete(URL_OLD_STORAGE + fileName);
        return httpClient.execute(oldStorageRequest).getStatusLine().getStatusCode();
    }

    private void runCleaner(String current) {
        int code;
        try {
            code = cleanFromOldStorage(current);
        } catch (IOException e) {
            code = -1;
            logger.error(e);
        }
        switch (code) {
            case 200:
                logger.info("Deleted: " + current);
                break;
            case 404:
                logger.info("Attempt to delete non-existing file. Skipping " + current);
                break;
            case 500:
                fileNames.offer(current);
                logger.error("Failed to delete due to server error: " + current + ". Trying again later...");
                break;
            default:
                fileNames.offer(current);
                logger.error("Failed to delete due to network error: " + current + ". Trying again later...");
                break;
        }
    }

    @Override
    public void run() {
        while (!fileNames.isEmpty()) {
            String current = fileNames.poll();
            runCleaner(current);
        }
    }
}
