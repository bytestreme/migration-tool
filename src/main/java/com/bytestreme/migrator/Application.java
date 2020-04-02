package com.bytestreme.migrator;

public class Application {

    public final static String BASE_URL = "http://localhost:8080/";

    public static void main(String[] args) {
        Worker.work("89.txt");
    }
}
