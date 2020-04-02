package com.bytestreme.migrator;

import com.bytestreme.migrator.service.Coordinator;
import com.bytestreme.migrator.service.impl.CoordinatorImpl;
import org.apache.log4j.Logger;

import static com.bytestreme.migrator.Constants.DEFAULT_WORKERS_NUM;

public class Application {

    private final static Logger logger = Logger.getLogger(Application.class);

    public static void main(String[] args) {
        int number;

        if (args.length == 0) {
            number = DEFAULT_WORKERS_NUM;
            logger.info("Workers number in `args` not provided, using default value (" + DEFAULT_WORKERS_NUM + ")");
        } else {
            number = Integer.parseInt(args[0]);
        }

        Coordinator coordinator = new CoordinatorImpl(number);
        coordinator.init();
        coordinator.migrate();
    }


}
