package com.anuj.nosqlconnector.logger;

import org.apache.log4j.Logger;

public interface Loggable {

    /**
     * <p>
     *     Method to provide the logger reference across the API module.
     * </p>
     * @return
     */
    default Logger logger(){
        return Logger.getLogger(this.getClass().getName());
    }
}
