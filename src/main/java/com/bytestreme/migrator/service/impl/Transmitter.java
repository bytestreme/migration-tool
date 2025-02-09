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

/**
 * Thread that will perform DELETE to the old storage
 */
public class Transmitter implements Worker {

    private final static Logger logger = Logger.getLogger(Transmitter.class);

    private Queue<String> fileNames;

    public Transmitter(Queue<String> fileNames) {
        this.fileNames = fileNames;
    }

    /**
     * @param fileName being processed
     * @return status code of file upload
     * @throws IOException in case of network or other IO errors
     */
    private int transmitFile(String fileName) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createMinimal();

        CloseableHttpResponse oldStorageResponse = performOldFileRequest(httpClient, URL_OLD_STORAGE + fileName);

        HttpPost newStorageRequest = new HttpPost(URL_NEW_STORAGE);
        HttpEntity newStoragePayload = MultipartEntityBuilder
                .create()
                .addBinaryBody(FORM_DATA_FIELD, oldStorageResponse.getEntity().getContent(),
                        ContentType.MULTIPART_FORM_DATA, fileName)
                .build();
        newStorageRequest.setEntity(newStoragePayload);
        CloseableHttpResponse newStorageResponse = httpClient.execute(newStorageRequest);
        return newStorageResponse.getStatusLine().getStatusCode();
    }

    /**
     * @param client being executing request
     * @param url full path of file to download
     * @return response being processed for file upload
     * @throws IOException in case of network or other IO errors
     */
    private CloseableHttpResponse performOldFileRequest(CloseableHttpClient client, String url) throws IOException {
        HttpGet oldStorageRequest = new HttpGet(url);
        return client.execute(oldStorageRequest);
    }

    /**
     * @param current file being processed
     */
    private void runTransmitter(String current) {
        int code;
        try {
            code = transmitFile(current);
        } catch (IOException e) {
            code = -1;
            logger.error(e);
        }
        switch (code) {
            case 200:
                logger.info("Finished: " + current);
                break;
            case 409:
                logger.info("Attempt to load existing file. Skipping " + current);
                break;
            case 500:
                fileNames.offer(current);
                logger.error("Failed to transmit due to server error: " + current + ". Trying again later...");
                break;
            default:
                fileNames.offer(current);
                logger.error("Failed to transmit due to network error: " + current + ". Trying again later...");
                break;
        }
    }

    /**
     * Execute until all files are transmitted
     */
    @Override
    public void run() {
        while (!fileNames.isEmpty()) {
            String current = fileNames.poll();
            runTransmitter(current);
        }
    }
}
