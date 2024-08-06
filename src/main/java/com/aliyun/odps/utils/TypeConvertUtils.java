/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.utils;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TypeConvertUtils {

    public static TypeInfo convertToMaxComputeType(String typeName) {
        if ("BOOLEAN".equals(typeName)) {
            return TypeInfoFactory.BOOLEAN;
        }
        if ("TINYINT".equals(typeName)) {
            return TypeInfoFactory.TINYINT;
        }
        if ("SMALLINT".equals(typeName) || "INT2".equals(typeName)) {
            return TypeInfoFactory.SMALLINT;
        }
        if ("BIGINT".equals(typeName) || "INT8".equals(typeName) || "UNSIGNED BIG INT".equals(typeName)) {
            return TypeInfoFactory.BIGINT;
        }
        if ("INT".equals(typeName) || "INTEGER".equals(typeName) || "MEDIUMINT".equals(typeName)) {
            return TypeInfoFactory.INT;
        }
        if ("DATE".equals(typeName)) {
            return TypeInfoFactory.DATE;
        }
        if ("DATETIME".equals(typeName)) {
            return TypeInfoFactory.DATETIME;
        }
        if ("TIMESTAMP".equals(typeName)) {
            return TypeInfoFactory.TIMESTAMP;
        }
        if ("DECIMAL".equals(typeName)) {
            return TypeInfoFactory.DECIMAL;
        }
        if ("DOUBLE".equals(typeName) || "DOUBLE PRECISION".equals(typeName)) {
            return TypeInfoFactory.DOUBLE;
        }
        if ("FLOAT".equals(typeName) || "REAL".equals(typeName)) {
            return TypeInfoFactory.FLOAT;
        }
        if ("CHARACTER".equals(typeName) || "NCHAR".equals(typeName) || "NATIVE CHARACTER".equals(typeName) ||
                "CHAR".equals(typeName) || "STRING".equals(typeName)) {
            return TypeInfoFactory.STRING;
        }
        if ("VARCHAR".equals(typeName) || "VARYING CHARACTER".equals(typeName) ||
                "NVARCHAR".equals(typeName) || "TEXT".equals(typeName)) {
            return TypeInfoFactory.STRING;
        }

        if ("BINARY".equals(typeName) || "BLOB".equals(typeName)) {
            return TypeInfoFactory.BINARY;
        }
        throw new UnsupportedOperationException("Unsupported type: " + typeName);
    }

    public static String convertToSqlLiteType(String typeName) {
        if (typeName.equalsIgnoreCase("STRING")) {
            return "TEXT";
        }
        return typeName;
    }

    public static Object convertToMaxComputeValue(int columnType, Object object) {
        if (object == null) {
            return null;
        }
        return switch (columnType) {
            case Types.TINYINT -> ((Number) object).byteValue();
            case Types.SMALLINT -> ((Number) object).shortValue();
            case Types.INTEGER -> ((Number) object).intValue();
            case Types.BIGINT -> ((Number) object).longValue();
            case Types.FLOAT, Types.REAL -> ((Number) object).floatValue();
            case Types.DOUBLE -> ((Number) object).doubleValue();
            case Types.DATE -> convertStringToLocalDate((String) object);
            default -> object;
        };
    }

    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static LocalDate convertStringToLocalDate(String dateString) {
        if (dateString == null) {
            return null;
        }
        try {
            return LocalDate.parse(dateString, DATE_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date format, expected yyyy-MM-dd", e);
        }
    }
}

