package com.bytestreme.migrator.struct;

import lombok.Getter;
import org.apache.log4j.Logger;

import static com.bytestreme.migrator.Constants.DEFAULT_WORKERS_NUM;

public class MigrationConfig {

    private final static Logger logger = Logger.getLogger(MigrationConfig.class);

    @Getter
    private int workersNumber;

    public MigrationConfig(String[] args) {
        this.workersNumber = getNumberOfWorkers(args);
    }

    private static int getNumberOfWorkers(String[] args) {
        int number = DEFAULT_WORKERS_NUM;

        if (args.length == 0) {
            logger.warn("Workers number in `args` not provided. Using default value (" + DEFAULT_WORKERS_NUM + ")");
        } else {
            try {
                number = Integer.parseInt(args[0]);
                logger.info("Workers number is set to " + number);
            } catch (NumberFormatException e) {
                logger.warn("Invalid argument for workers number provided. Using default value (" + DEFAULT_WORKERS_NUM + ")");
            }
        }
        return number;
    }

}
