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

package tech.dingxin.maxcompute.utils;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.CodedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Deserializer {
    public static List<Object[]> deserializeData(InputStream data, List<TypeInfo> odpsType) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(data);
        List<Object[]> records = new ArrayList<>();

        Object[] record = new Object[odpsType.size()];
        while (true) {
            int index = getNextFieldNumber(input);
            if (index == 33554430) {
                break;
            }
            if (index == 33553408) {
                records.add(record);
                // no check crc
                input.readUInt32();
                record = new Object[odpsType.size()];
            }
            if (index > odpsType.size()) {
                if (index == odpsType.size() + 1 || index == odpsType.size() + 2) {
                    readField(input, TypeInfoFactory.BIGINT);
                } else if (index == odpsType.size() + 3) {
                    readField(input, TypeInfoFactory.TINYINT);
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
            case BIGINT, INT, TINYINT -> {
                return input.readSInt64();
            }
            case DOUBLE -> {
                return input.readDouble();
            }
            case DATETIME -> {
                return Instant.ofEpochMilli(input.readSInt64()).atZone(ZoneId.of("UTC"));
            }
            case STRING, DECIMAL -> {
                int length = input.readRawVarint32();
                String str = new String(input.readRawBytes(length), StandardCharsets.UTF_8);
                if (typeInfo.getOdpsType() == OdpsType.DECIMAL) {
                    return new BigDecimal(str);
                }
                return str;
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
