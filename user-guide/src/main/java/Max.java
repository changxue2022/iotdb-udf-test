import java.io.IOException;

import org.apache.commons.logging.Log;
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
 * input: int,long, 如果输入非int, 则会报错
 * description: get the max value
 * output: long
 */
public class Max implements UDTF {

    private static final Logger LOGGER = LoggerFactory.getLogger(Max.class);
    private Long time;
    private Long value = null;

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        validator.validateInputSeriesDataType(0, Type.INT32, Type.INT64);
    }

    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
        configurations
                .setOutputDataType(Type.INT64)
                .setAccessStrategy(new RowByRowAccessStrategy());
    }

    @Override
    public void transform(Row row, PointCollector collector) throws IOException {
        if (row.isNull(0)) {
            return;
        }
        LOGGER.info("#### Max transform [{}] #####", row.getTime());
        if (Type.INT32.equals(row.getDataType(0))) {
            int candidateValue = row.getInt(0);
            if (time == null || value < candidateValue) {
                time = row.getTime();
                value = Long.valueOf(candidateValue);
            }
        } else if (Type.INT64.equals(row.getDataType(0))) {
            long candidateValue = row.getLong(0);
            if (time == null || value < candidateValue) {
                time = row.getTime();
                value = candidateValue;
            }
        }
    }

    @Override
    public void terminate(PointCollector collector) throws IOException {
        LOGGER.info("#### Max terminate #####");
        if (time != null) {
            collector.putLong(time, value);
        }
    }
}
