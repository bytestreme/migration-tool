package com.bytestreme.migrator;

import com.bytestreme.migrator.service.impl.CoordinatorImpl;
import org.apache.log4j.Logger;

import static com.bytestreme.migrator.Constants.DEFAULT_WORKERS_NUM;

public class Application {

    private final static Logger logger = Logger.getLogger(Application.class);

    public static void main(String[] args) {
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

        new CoordinatorImpl(number)
                .init()
                .migrate();

    }


}
