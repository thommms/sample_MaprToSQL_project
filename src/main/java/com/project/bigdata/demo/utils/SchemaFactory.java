package com.project.bigdata.demo.utils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class SchemaFactory implements Serializable {

    private static final long serialVersionUID = 6734015607512574479L;

    public static StructType getSchema() {
        StructType structType = new StructType();
        return structType.add("field1", DataTypes.StringType).add("field2", DataTypes.StringType).add("field3", DataTypes.StringType)
                .add("field4", DataTypes.StringType).add("date_col", DataTypes.StringType);

        }
}
