package com.timecho.udf.exception;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionTransform implements UDTF {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionTransform.class);

    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("########## ExceptionTransform # beforeStart ##########");
        configurations
                .setOutputDataType(Type.DOUBLE)
                .setAccessStrategy(new RowByRowAccessStrategy());   }

    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        LOGGER.info("########## ExceptionTransform # transform ##########");
        throw new RuntimeException("ExceptionTransform exception");
    }
}
