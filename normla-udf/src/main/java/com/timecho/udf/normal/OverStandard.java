package com.timecho.udf.normal;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * input: int32
 * attributes: standard value int32， round float
 * 挑出那些比standard value 大的值
 * output: int32
 */
public class OverStandard implements UDTF {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverStandard.class);
    private int standard;
    private float round;

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        LOGGER.info("######## OverStandard # validate #########");
        validator.validateInputSeriesDataType(0, Type.INT32);
        validator.validateRequiredAttribute("standard");
        validator.validateRequiredAttribute("round");
    }
    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("######## OverStandard # beforeStart #########");
        configurations
                .setOutputDataType(Type.INT32)
                .setAccessStrategy(new RowByRowAccessStrategy());
    }
    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        LOGGER.info("######## OverStandard # transform #########");
        if (! row.isNull(0)) {
            if (Math.abs(row.getInt(0) - standard)/standard > round) {
                collector.putInt(row.getTime(), row.getInt(0));
            }
        }
    }

    @Override
    public void beforeDestroy() {
        LOGGER.info("######## OverStandard # beforeDestroy #########");
    }
}
