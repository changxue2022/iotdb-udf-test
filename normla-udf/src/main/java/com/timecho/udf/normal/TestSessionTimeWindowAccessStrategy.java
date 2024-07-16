package com.timecho.udf.normal;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SessionTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input: Text
 * Description: count the "standard" value
 * Return: float
 */
public class TestSessionTimeWindowAccessStrategy implements UDTF {
    private String standardValue;
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSessionTimeWindowAccessStrategy.class);
    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("###### TestSessionTimeWindow # beforeStart #######");
        LOGGER.info("attributes: {}", parameters.getAttributes().toString());
        this.standardValue = parameters.getString("standard");
        if (parameters.hasAttribute("start") && parameters.hasAttribute("end")) {
            configurations
                    .setOutputDataType(Type.FLOAT)
                    .setAccessStrategy(new SessionTimeWindowAccessStrategy(parameters.getLong("start"),
                            parameters.getLong("end"), parameters.getInt("max_interval")));

        } else if (parameters.hasAttribute("start") || parameters.hasAttribute("end")) {
            throw new RuntimeException("start and end must be both given. ");
        } else {
            configurations
                    .setOutputDataType(Type.FLOAT)
                    .setAccessStrategy(new SessionTimeWindowAccessStrategy(parameters.getLong("max_interval")));
        }
    }

    @Override
    public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
        LOGGER.info("######### TestSessionTimeWindow # [{}] {} ########", rowWindow.getRow(0).getTime(), rowWindow.windowSize());
        int result = 0;
        for (int i=0; i < rowWindow.windowSize(); i++) {
            if (! rowWindow.getRow(i).isNull(0) && this.standardValue.equals(rowWindow.getRow(i).getBinary(0).getStringValue())) {
                result++;
            }
        }
        collector.putFloat(rowWindow.getRow(0).getTime(), result);
//        collector.putInt(rowWindow.getRow(0).getTime(), result);
    }

    @Override
    public void beforeDestroy() {
        LOGGER.info("###### TestSessionTimeWindow # beforeDestroy #######");
    }

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        LOGGER.info("###### TestSessionTimeWindow # validate #######");
        validator.validateInputSeriesDataType(0, Type.TEXT);
        validator.validateRequiredAttribute("max_interval");
        validator.validateRequiredAttribute("standard");
    }
}
