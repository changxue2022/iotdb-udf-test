import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
/**
 * Input: boolean
 * output: int
 */

public class SameNameTest implements UDTF {
    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations) throws Exception {
        udtfConfigurations.setOutputDataType(Type.INT32).setAccessStrategy(new RowByRowAccessStrategy());
    }

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        validator.validateInputSeriesDataType(0, Type.BOOLEAN);
    }
}
