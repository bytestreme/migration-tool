package com.bytestreme.migrator;

import com.bytestreme.migrator.service.impl.CoordinatorImpl;
import com.bytestreme.migrator.struct.MigrationConfig;

public class Application {

    public static void main(String[] args) {
        MigrationConfig config = new MigrationConfig(args);

        new CoordinatorImpl(config)
                .init()
                .migrate();
    }

}
