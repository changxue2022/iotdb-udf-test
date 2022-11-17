import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotImplementUTDF {
    private static final Logger LOGGER = LoggerFactory.getLogger(NotImplementUTDF.class);

    public void validate(UDFParameterValidator validator) throws Exception {
        LOGGER.info("######## validate #########");
        validator.validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
    }
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("########## ExceptionTerminate # beforeStart ##########");
        configurations
                .setOutputDataType(Type.DOUBLE)
                .setAccessStrategy(new RowByRowAccessStrategy());
    }

    public void transform(Row row, PointCollector collector) throws Exception {
        LOGGER.info("########## ExceptionTerminate # transform ##########");
        LOGGER.info("[{}] {}", row.getTime(), row.getInt(0));
    }

    public void terminate(PointCollector collector) throws Exception {
        LOGGER.info("########## ExceptionTerminate # beforeStart ##########");
    }
    public void beforeDestroy() {
        LOGGER.info("######## beforeDestroy #########");
    }

}
