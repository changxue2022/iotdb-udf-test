package com.timecho.udf.exception;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionValidate implements UDTF {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionValidate.class);

    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("########## ExceptionValidate # beforeStart ##########");
        configurations
                .setOutputDataType(Type.DOUBLE)
                .setAccessStrategy(new RowByRowAccessStrategy());
    }

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        LOGGER.info("########## ExceptionValidate # validate ##########");
        throw new RuntimeException("validate exception");
    }
}
