package com.bytestreme.migrator.service.impl;

import com.bytestreme.migrator.service.Worker;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Queue;

import static com.bytestreme.migrator.Constants.*;


public class WorkerImpl implements Worker {

    private final static Logger logger = Logger.getLogger(WorkerImpl.class);

    private Queue<String> fileNames;

    public WorkerImpl(Queue<String> fileNames) {
        this.fileNames = fileNames;
    }

    private boolean work(String fileName) {

        try (CloseableHttpClient httpClient = HttpClients.createMinimal()) {

            HttpGet oldStorageRequest = new HttpGet(URL_OLD_STORAGE + fileName);

            CloseableHttpResponse oldStorageResponse = httpClient.execute(oldStorageRequest);

            HttpPost newStorageRequest = new HttpPost(URL_NEW_STORAGE);

            HttpEntity newStoragePayload = MultipartEntityBuilder
                    .create()
                    .addBinaryBody(FORM_DATA_FIELD, oldStorageResponse.getEntity().getContent(),
                            ContentType.MULTIPART_FORM_DATA, fileName)
                    .build();
            newStorageRequest.setEntity(newStoragePayload);

            CloseableHttpResponse newStorageResponse = httpClient.execute(newStorageRequest);

            return newStorageResponse.getStatusLine().getStatusCode() == 200;

        } catch (IOException e) {
            logger.error(e);
            return false;
        }
    }

    @Override
    public void run() {
        while (!fileNames.isEmpty()) {
            String current = fileNames.poll();
            boolean success = work(current);
            if (!success) {
                fileNames.offer(current);
                logger.error("Failed to transmit: " + current + ". Trying again later...");
            } else {
                logger.info("Finished: " + current);
            }
        }
    }

}

