package com.timecho.udf.normal;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Input: Long
 * Description: sum
 * Return: Long
 */
public class TestSlidingTimeWindowAccessStrategy implements UDTF {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSlidingTimeWindowAccessStrategy.class);
    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("###### TestSlidingTimeWindow # beforeStart #######");
        LOGGER.info("attributes: {}", parameters.getAttributes().toString());
        if (parameters.hasAttribute("start") && parameters.hasAttribute("end")) {
            if (parameters.hasAttribute("step")) {
                configurations
                        .setOutputDataType(Type.INT64)
                        .setAccessStrategy(new SlidingTimeWindowAccessStrategy(parameters.getInt("interval"),
                                parameters.getInt("step"),
                                parameters.getLong("start"),
                                parameters.getLong("end")));
            } else {
                configurations
                        .setOutputDataType(Type.INT64)
                        .setAccessStrategy(new SlidingTimeWindowAccessStrategy(parameters.getInt("interval"),
                                parameters.getInt("interval"),
                                parameters.getLong("start"),
                                parameters.getLong("end")));
            }
        } else if (parameters.hasAttribute("start") || parameters.hasAttribute("end")) {
            throw new RuntimeException("start and end must be both existed. ");
        } else if (parameters.hasAttribute("step")) {
            configurations
                    .setOutputDataType(Type.INT64)
                    .setAccessStrategy(new SlidingTimeWindowAccessStrategy(parameters.getInt("interval"),
                            parameters.getInt("step")));
        } else {
            configurations
                    .setOutputDataType(Type.INT64)
                    .setAccessStrategy(new SlidingTimeWindowAccessStrategy(parameters.getInt("interval")));
        }
    }

    @Override
    public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
        LOGGER.info("######### TestSlidingTimeWindow # [{}] {} ########", rowWindow.getRow(0).getTime(), rowWindow.windowSize());
        long result = 0;
        for (int i=0; i < rowWindow.windowSize(); i++) {
            if (!rowWindow.getRow(i).isNull(0)) {
                result = result + rowWindow.getRow(i).getLong(0);
            }
        }
        collector.putLong(rowWindow.getRow(0).getTime(), result);
    }

    @Override
    public void beforeDestroy() {
        LOGGER.info("###### TestSlidingTimeWindow # beforeDestroy #######");
    }

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        validator.validateRequiredAttribute("interval");
        validator.validateInputSeriesDataType(0, Type.INT64);

    }
}
