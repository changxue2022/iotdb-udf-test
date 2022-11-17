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
public class EnumBug implements UDTF {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnumBug.class);
    private Double totalValue = 0.0;

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        LOGGER.info("######## validate #########");
        validator.validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
    }
    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("######## beforeStart #########");
        configurations
                .setOutputDataType(Type.DOUBLE)
                .setAccessStrategy(new RowByRowAccessStrategy());
    }
    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        LOGGER.info("######## transform #########"+row.getDataType(0));
        if (row.isNull(0)) {
            return;
        }
        switch (row.getDataType(0)) {
            case INT32:
                totalValue = totalValue + row.getInt(0);
                break;
            case INT64:
                totalValue = totalValue + row.getLong(0);
                break;
            case FLOAT:
                totalValue = totalValue + row.getFloat(0);
                break;
            case DOUBLE:
                totalValue = totalValue + row.getDouble(0);
                break;
            default:
                LOGGER.error("******* Bad data type!********");
        }
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        LOGGER.info("######## terminate #########");
        collector.putDouble(1L, totalValue);
    }

    @Override
    public void beforeDestroy() {
        LOGGER.info("######## beforeDestroy #########");
    }
}
