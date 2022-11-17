package com.timecho.udf.exception;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionBeforeStart implements UDTF {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionBeforeStart.class);

    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations) throws Exception {
        LOGGER.info("########## ExceptionBeforeStart # beforeStart ##########");
        throw new RuntimeException("before start exception");
    }

}
