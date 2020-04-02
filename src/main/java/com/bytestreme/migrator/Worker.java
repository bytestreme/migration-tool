package com.bytestreme.migrator;

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

import static com.bytestreme.migrator.Application.BASE_URL;


public class Worker {

    private final static String UPLOAD_BASE_URL = BASE_URL + "newStorage/files";
    private final static String DOWNLOAD_BASE_URL = BASE_URL + "oldStorage/files/";

    private final static String FORM_DATA_FIELD = "file";

    private final static Logger logger = Logger.getLogger(Worker.class);

    public static void work(String fileName) {

        try (CloseableHttpClient httpClient = HttpClients.createMinimal()) {

            HttpGet oldStorageRequest = new HttpGet(DOWNLOAD_BASE_URL + fileName);

            CloseableHttpResponse oldStorageResponse = httpClient.execute(oldStorageRequest);

            HttpPost newStorageRequest = new HttpPost(UPLOAD_BASE_URL);

            HttpEntity newStoragePayload = MultipartEntityBuilder
                    .create()
                    .addBinaryBody(FORM_DATA_FIELD, oldStorageResponse.getEntity().getContent(),
                            ContentType.MULTIPART_FORM_DATA, fileName)
                    .build();
            newStorageRequest.setEntity(newStoragePayload);

            CloseableHttpResponse newStorageResponse = httpClient.execute(newStorageRequest);

            logger.info(fileName + " => " + (newStorageResponse.getStatusLine().getStatusCode() == 200 ? "SUCCESS" : "FAILED"));

        } catch (IOException e) {
            logger.error(e);
        }

    }

}

