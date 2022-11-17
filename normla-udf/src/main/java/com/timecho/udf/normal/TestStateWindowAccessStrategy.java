package com.timecho.udf.normal;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input: Boolean
 * Description: the line count
 * Return: Text
 */
public class TestStateWindowAccessStrategy implements UDTF {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestStateWindowAccessStrategy.class);
    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("###### TestStateWindow # beforeStart #######");
        LOGGER.info("attributes: {}", parameters.getAttributes().toString());
        configurations
                .setOutputDataType(Type.TEXT)
                .setAccessStrategy(new StateWindowAccessStrategy());
    }

    @Override
    public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
        LOGGER.info("######### TestSlidingTimeWindow # [{}] {} ########", rowWindow.getRow(0).getTime(), rowWindow.windowSize());
        collector.putString(rowWindow.getRow(0).getTime(), String.valueOf(rowWindow.windowSize()));
    }

    @Override
    public void beforeDestroy() {
        LOGGER.info("###### TestStateWindow # beforeDestroy #######");
    }

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        validator.validateInputSeriesDataType(0, Type.BOOLEAN);
    }
}
