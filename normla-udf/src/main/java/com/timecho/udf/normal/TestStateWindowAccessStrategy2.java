package com.timecho.udf.normal;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.StateWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input: double
 * Description: the line count
 * Return: float
 */
public class TestStateWindowAccessStrategy2 implements UDTF {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestStateWindowAccessStrategy2.class);
    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("###### TestStateWindow # beforeStart #######");
        LOGGER.info("attributes: {}", parameters.getAttributes().toString());
        if (parameters.hasAttribute("start") && parameters.hasAttribute("end")) {
            configurations
                    .setOutputDataType(Type.FLOAT)
                    .setAccessStrategy(new StateWindowAccessStrategy(parameters.getLong("start"),
                            parameters.getLong("end"), parameters.getDouble("delta")));
        } else if (parameters.hasAttribute("start") || parameters.hasAttribute("end")) {
            throw new RuntimeException("start and end must be both given. ");
        } else {
            configurations
                    .setOutputDataType(Type.FLOAT)
                    .setAccessStrategy(new StateWindowAccessStrategy(parameters.getDouble("delta")));
        }
    }

    @Override
    public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
        LOGGER.info("######### TestStateWindow # [{}] {} ########", rowWindow.getRow(0).getTime(), rowWindow.windowSize());
        collector.putFloat(rowWindow.getRow(0).getTime(), rowWindow.windowSize());
    }

    @Override
    public void beforeDestroy() {
        LOGGER.info("###### TestStateWindow # beforeDestroy #######");
    }

    @Override
    public void validate(UDFParameterValidator validator) {
        validator.validateInputSeriesDataType(0, Type.DOUBLE);
        validator.validateRequiredAttribute("delta");
    }
}
