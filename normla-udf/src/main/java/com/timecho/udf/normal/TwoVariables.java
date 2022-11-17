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
    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        if (row.isNull(0) && row.isNull(1)) {
            collector.putBoolean(row.getTime(), true);
            return;
        } else if (row.isNull(0) || row.isNull(1)){
            collector.putBoolean(row.getTime(), false);
            return;
        }
        String var1 = "";
        String var2 = "";
        if (Type.INT32.equals(row.getDataType(0))) {
            var1 = String.valueOf(row.getInt(0));
        } else if (Type.INT64.equals(row.getDataType(0))) {
            var1 = String.valueOf(row.getLong(0));
        } else if (Type.FLOAT.equals(row.getDataType(0))) {
            var1 = String.valueOf(row.getFloat(0));
        } else if (Type.DOUBLE.equals(row.getDataType(0))) {
            var1 = String.valueOf(row.getDouble(0));
        } else if (Type.BOOLEAN.equals(row.getDataType(0))) {
            var1 = String.valueOf(row.getBoolean(0));
        } else if (Type.TEXT.equals(row.getDataType(0))) {
            var1 = String.valueOf(row.getBinary(0));
        } else {
            LOGGER.error("******* Bad data type!********");
        }
        if (Type.INT32.equals(row.getDataType(1))) {
            var2 = String.valueOf(row.getInt(1));
        } else if (Type.INT64.equals(row.getDataType(1))) {
            var2 = String.valueOf(row.getLong(1));
        } else if (Type.FLOAT.equals(row.getDataType(1))) {
            var2 = String.valueOf(row.getFloat(1));
        } else if (Type.DOUBLE.equals(row.getDataType(1))) {
            var2 = String.valueOf(row.getDouble(1));
        } else if (Type.BOOLEAN.equals(row.getDataType(1))) {
            var2 = String.valueOf(row.getBoolean(1));
        } else if (Type.TEXT.equals(row.getDataType(1))) {
            var2 = String.valueOf(row.getBinary(1));
        } else {
            LOGGER.error("******* Bad data type!********");
        }
        LOGGER.info("##### var1="+var1);
        LOGGER.info("##### var2="+var2);
        collector.putBoolean(row.getTime(), var1.equals(var2));
    }

}
