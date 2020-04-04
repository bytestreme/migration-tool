package com.bytestreme.migrator.service;

public interface Coordinator {

    void migrate();

    Coordinator init();
}
