package org.apache.iotdb.udf.http;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * input: int
 * description: compare two value
 * output: boolean
 */
public class TestObjectRow implements UDTF {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestObjectRow.class);
    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations configurations) throws Exception {
        LOGGER.info("####### TestObjectRow # beforeStart ######");
        configurations
                .setOutputDataType(Type.BOOLEAN)
                .setAccessStrategy(new MappableRowByRowAccessStrategy());
    }

    @Override
    public Object transform(Row row) throws Exception {
        LOGGER.info("###### TestObjectRow # transform  #######");
        if ( row.isNull(0) || row.isNull(1)) {
            return false;
        } else if (row.isNull(0) && row.isNull(1)) {
            return true;
        } else {
            LOGGER.info("{} == {}", row.getInt(0), row.getInt(1));
            return row.getInt(0) == row.getInt(1);
        }
    }

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        LOGGER.info("##### TestObjectRow # validate ######");
        validator.validateInputSeriesDataType(0, Type.INT32);
        validator.validateInputSeriesDataType(1, Type.INT32);
    }
}
