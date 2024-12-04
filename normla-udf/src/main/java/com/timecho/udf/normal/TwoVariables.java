package com.timecho.udf.normal;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 比较2个参数的值是否相等
 */
public class TwoVariables implements UDTF {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwoVariables.class);

    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("########## TwoVariables # beforeStart ##########");
        configurations
                .setOutputDataType(Type.BOOLEAN)
                .setAccessStrategy(new RowByRowAccessStrategy());

    }

    private String getValue(Row row, int index) throws IOException {
        switch (row.getDataType(index)) {
            case INT32:
            case DATE:
                return String.valueOf(row.getInt(index));
            case INT64:
            case TIMESTAMP:
                return String.valueOf(row.getLong(index));
            case FLOAT:
                return String.valueOf(row.getFloat(index));
            case DOUBLE:
                return String.valueOf(row.getDouble(index));
            case BOOLEAN:
                return String.valueOf(row.getBoolean(index));
            case TEXT:
            case STRING:
            case BLOB:
                return String.valueOf(row.getBinary(index));
            default:
                LOGGER.error("******* Bad data type!********");
                return null;
        }
    }
    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        if (row.isNull(0) && row.isNull(1)) {
            collector.putBoolean(row.getTime(), true);
            return;
        } else if (row.isNull(0) || row.isNull(1)){
            collector.putBoolean(row.getTime(), false);
            return;
        }
        String var1 = getValue(row, 0);
        String var2 = getValue(row, 1);

        LOGGER.info("##### var1="+var1);
        LOGGER.info("##### var2="+var2);
        collector.putBoolean(row.getTime(), var1.equals(var2));
    }

}
