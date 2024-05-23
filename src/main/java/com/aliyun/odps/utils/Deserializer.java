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

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.entity.RowData;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.CodedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Deserializer {
    public static List<RowData> deserializeData(InputStream data, List<TypeInfo> odpsType) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(data);
        List<RowData> records = new ArrayList<>();

        Object[] record = new Object[odpsType.size()];
        RowData.RowKind rowKind = RowData.RowKind.UPSERT;
        while (true) {
            int index = getNextFieldNumber(input);
            if (index == 33554430) {
                break;
            }
            if (index == 33553408) {
                records.add(new RowData(record, rowKind));
                // no check crc
                input.readUInt32();
                record = new Object[odpsType.size()];
            }
            if (index > odpsType.size()) {
                if (index == odpsType.size() + 1 || index == odpsType.size() + 2) {
                    readField(input, TypeInfoFactory.BIGINT);
                } else if (index == odpsType.size() + 3) {
                    byte r = (byte) readField(input, TypeInfoFactory.TINYINT);
                    if (r == 'D') {
                        rowKind = RowData.RowKind.DELETE;
                    } else if (r == 'U') {
                        rowKind = RowData.RowKind.UPSERT;
                    }
                } else if (index == odpsType.size() + 4 || index == odpsType.size() + 5) {
                    readField(input, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT));
                }
            } else {
                Object value = readField(input, odpsType.get(index - 1));
                record[index - 1] = value;
            }
        }
        return records;
    }

    private static Object readField(CodedInputStream input, TypeInfo typeInfo)
            throws IOException {
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN -> {
                return input.readBool();
            }
            case BIGINT -> {
                return input.readSInt64();
            }
            case INT -> {
                return ((Number) input.readSInt64()).intValue();
            }
            case SMALLINT -> {
                return ((Number) input.readSInt64()).shortValue();
            }
            case TINYINT -> {
                return ((Number) input.readSInt64()).byteValue();
            }
            case DOUBLE -> {
                return input.readDouble();
            }
            case FLOAT -> {
                return input.readFloat();
            }
            case DATETIME -> {
                return Instant.ofEpochMilli(input.readSInt64()).atZone(ZoneId.of("UTC"));
            }
            case DATE -> {
                return LocalDate.ofEpochDay(input.readSInt64());
            }
            case TIMESTAMP -> {
                return Instant.ofEpochSecond(input.readSInt64(), input.readSInt32());
            }
            case TIMESTAMP_NTZ -> {
                return LocalDateTime.ofInstant(Instant.ofEpochSecond(input.readSInt64(), input.readSInt32()),
                        ZoneId.of("UTC"));
            }
            case CHAR, VARCHAR, STRING, DECIMAL -> {
                int length = input.readRawVarint32();
                String str = new String(input.readRawBytes(length), StandardCharsets.UTF_8);
                if (typeInfo.getOdpsType() == OdpsType.DECIMAL) {
                    return new BigDecimal(str);
                }
                return str;
            }
            case BINARY -> {
                int length = input.readRawVarint32();
                return input.readRawBytes(length);
            }
            case ARRAY -> {
                int size = input.readInt32();
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    boolean isNull = input.readBool();
                    if (isNull) {
                        list.add(null);
                    } else {
                        list.add(readField(input, ((ArrayTypeInfo) typeInfo).getElementTypeInfo()));
                    }
                }
                return list;
            }
            default -> throw new IOException("Invalid data type: " + typeInfo.getOdpsType());
        }
    }

    private static int getNextFieldNumber(CodedInputStream input) throws IOException {
        int tag = input.readTag();
        return tag >>> 3;
    }
}
