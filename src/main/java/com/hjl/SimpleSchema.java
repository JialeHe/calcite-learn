package com.hjl;

import com.google.common.collect.Maps;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @Description
 * @Author jiale.he
 * @Date 2022-08-09 13:51 周二
 */
public class SimpleSchema extends AbstractSchema {
    private final String schemaName;
    private final Map<String, Table> tableMap;

    public SimpleSchema(String schemaName, Map<String, Table> tableMap) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    public static Builder newBuilder(String schemaName) {
        return new Builder(schemaName);
    }

    public static final class Builder {
        private final String schemaName;
        private final Map<String, Table> tableMap = Maps.newHashMap();

        private Builder(String schemaName) {
            if (StringUtils.isEmpty(schemaName)) {
                throw new IllegalArgumentException("Schema name cannot be null or empty");
            }
            this.schemaName = schemaName;
        }

        public Builder addTable(SimpleTable table) {
            if (tableMap.containsKey(table.getTableName())) {
                throw new IllegalArgumentException("Table already defined: " + table.getTableName());
            }
            tableMap.put(table.getTableName(), table);
            return this;
        }

        public SimpleSchema build() {
            return new SimpleSchema(schemaName, tableMap);
        }
    }
}
