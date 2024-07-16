package com.timecho.udf.normal;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数值类型累加
 */
public class MySum implements UDTF {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySum.class);
    private Double totalValue = 0.0;
    private Long timestamp;

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        LOGGER.info("######## MySum validate #########");
        validator.validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
    }
    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("######## MySum beforeStart #########");
        configurations
                .setOutputDataType(Type.DOUBLE)
                .setAccessStrategy(new RowByRowAccessStrategy());
    }
    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        LOGGER.info("######## MySum transform ######### [{}]", row.getTime());
        if (timestamp == null) {
            timestamp = row.getTime();
        }
        if (row.isNull(0)) {
            return;
        }
        if (Type.INT32.equals(row.getDataType(0))) {
            totalValue = totalValue + row.getInt(0);
        } else if (Type.INT64.equals(row.getDataType(0))) {
                totalValue = totalValue + row.getLong(0);
         } else if (Type.FLOAT.equals(row.getDataType(0))) {
                totalValue = totalValue + row.getFloat(0);
        } else if (Type.DOUBLE.equals(row.getDataType(0))) {
            totalValue = totalValue + row.getDouble(0);
        } else {
            LOGGER.error("******* Bad data type!********");
        }
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        LOGGER.info("######## MySum terminate #########");
        collector.putDouble(timestamp, totalValue);
    }

    @Override
    public void beforeDestroy() {
        LOGGER.info("######## MySum beforeDestroy #########");
    }
}
