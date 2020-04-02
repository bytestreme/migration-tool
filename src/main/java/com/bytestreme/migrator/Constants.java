package com.bytestreme.migrator;

public class Constants {
    public final static String BASE_URL = "http://localhost:8080/";
    public final static String URL_NEW_STORAGE = BASE_URL + "newStorage/files";
    public final static String URL_OLD_STORAGE = BASE_URL + "oldStorage/files/";

    public final static String FORM_DATA_FIELD = "file";

    public final static Long RETRY_COOLDOWN_MILLIS = 5000L;

    public final static int DEFAULT_WORKERS_NUM = 10;

}
