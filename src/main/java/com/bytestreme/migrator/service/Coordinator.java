package com.bytestreme.migrator.service;

public interface Coordinator {

    /**
     * Starts migration
     * @throws IllegalStateException in case of not prior init()
     */
    void migrate();

    /**
     * @return configured object with initialized files list
     */
    Coordinator init();
}
