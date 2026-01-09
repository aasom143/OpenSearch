/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.iceberg;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts Apache Arrow schemas to Apache Iceberg schemas.
 * Handles mapping between Arrow and Iceberg type systems.
 */
public class ArrowToIcebergSchemaConverter {
    private static final Logger logger = LogManager.getLogger(ArrowToIcebergSchemaConverter.class);

    /**
     * Convert Arrow schema to Iceberg schema.
     * 
     * @param arrowSchema Arrow schema from ParquetWriter
     * @return Equivalent Iceberg schema
     */
    public static org.apache.iceberg.Schema convert(Schema arrowSchema) {
        List<Types.NestedField> fields = new ArrayList<>();
        int fieldId = 1;
        
        for (Field arrowField : arrowSchema.getFields()) {
            try {
                Type icebergType = convertType(arrowField.getType());
                boolean nullable = arrowField.isNullable();
                
                Types.NestedField icebergField = nullable
                    ? Types.NestedField.optional(fieldId++, arrowField.getName(), icebergType)
                    : Types.NestedField.required(fieldId++, arrowField.getName(), icebergType);
                
                fields.add(icebergField);
            } catch (UnsupportedOperationException e) {
                logger.warn("Skipping unsupported field '{}' of type {}: {}", 
                           arrowField.getName(), arrowField.getType(), e.getMessage());
            }
        }
        
        if (fields.isEmpty()) {
            // Fallback to minimal schema if no fields could be converted
            logger.warn("No fields converted successfully, using minimal schema");
            fields.add(Types.NestedField.required(1, "row_id", Types.LongType.get()));
            fields.add(Types.NestedField.optional(2, "data", Types.StringType.get()));
        }
        
        return new org.apache.iceberg.Schema(fields);
    }

    /**
     * Convert Arrow type to Iceberg type.
     */
    private static Type convertType(ArrowType arrowType) {
        ArrowType.ArrowTypeID typeId = arrowType.getTypeID();
        
        switch (typeId) {
            case Bool:
                return Types.BooleanType.get();
                
            case Int:
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                if (intType.getBitWidth() == 32) {
                    return Types.IntegerType.get();
                } else if (intType.getBitWidth() == 64) {
                    return Types.LongType.get();
                } else if (intType.getBitWidth() == 16) {
                    return Types.IntegerType.get(); // Map short to int
                } else {
                    return Types.IntegerType.get(); // Default to int
                }
                
            case FloatingPoint:
                ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
                // Check bit width for float vs double
                switch (floatType.getPrecision()) {
                    case HALF:
                    case SINGLE:
                        return Types.FloatType.get();
                    case DOUBLE:
                    default:
                        return Types.DoubleType.get();
                }
                
            case Utf8:
            case LargeUtf8:
                return Types.StringType.get();
                
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
                return Types.BinaryType.get();
                
            case Date:
                return Types.DateType.get();
                
            case Time:
                return Types.TimeType.get();
                
            case Timestamp:
                ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
                // Iceberg timestamp is always microseconds
                return Types.TimestampType.withoutZone();
                
            case Decimal:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
                return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
                
            case List:
                // For lists, use string as element type (simplified)
                return Types.ListType.ofOptional(1, Types.StringType.get());
                
            case Struct:
                // For structs, use string representation (simplified)
                return Types.StringType.get();
                
            case Map:
                // For maps, use string for both key and value (simplified)
                return Types.MapType.ofOptional(1, 2, Types.StringType.get(), Types.StringType.get());
                
            default:
                logger.warn("Unsupported Arrow type: {}, using String", typeId);
                return Types.StringType.get(); // Fallback to string for unsupported types
        }
    }
}
