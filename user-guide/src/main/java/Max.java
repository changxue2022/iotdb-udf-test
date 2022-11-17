import java.io.IOException;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

/**
 * input: int
 * description: get the max value
 * output: int
 */
public class Max implements UDTF {

    private Long time;
    private int value;

    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) {
        configurations
                .setOutputDataType(Type.INT32)
                .setAccessStrategy(new RowByRowAccessStrategy());
    }

    @Override
    public void transform(Row row, PointCollector collector) throws IOException {
        int candidateValue = row.getInt(0);
        if (time == null || value < candidateValue) {
            time = row.getTime();
            value = candidateValue;
        }
    }

    @Override
    public void terminate(PointCollector collector) throws IOException {
        if (time != null) {
            collector.putInt(time, value);
        }
    }
}
