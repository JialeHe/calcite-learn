package com.hjl;

import com.google.common.collect.Lists;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.CsvEnumerator;
import org.apache.calcite.adapter.file.CsvFieldType;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Description
 * @Author jiale.he
 * @Date 2022-08-09 13:04 周二
 */
public class SimpleTable extends AbstractTable implements ScannableTable, ProjectableFilterableTable {

    private final String tableName;
    private final String filePath;
    private final List<String> fieldNames;
    private final List<SqlTypeName> fieldTypes;
    private final SimpleTableStatistic statistic;

    private RelDataType rowType;

    @Override
    public Statistic getStatistic() {
        return statistic;
    }

    public String getTableName() {
        return tableName;
    }

    public SimpleTable(String tableName,
                       String filePath,
                       List<String> fieldNames,
                       List<SqlTypeName> fieldTypes,
                       SimpleTableStatistic statistic) {
        this.tableName = tableName;
        this.filePath = filePath;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.statistic = statistic;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> list, int @Nullable [] ints) {
        File file = new File(filePath);
        Source source = Sources.of(file);
        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        ImmutableIntList fields = ImmutableIntList.identity(fieldTypes.size());

        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(source, cancelFlag, false, null,
                        CsvEnumerator.arrayConverter(getCsvFieldTypes(), fields, false));
            }
        };
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {

        File file = new File(filePath);
        Source source = Sources.of(file);
        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        ImmutableIntList fields = ImmutableIntList.identity(fieldTypes.size());
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(source, cancelFlag, false, null,
                        CsvEnumerator.arrayConverter(getCsvFieldTypes(), fields, false));
            }
        };
    }

    private List<CsvFieldType> getCsvFieldTypes() {
        List<CsvFieldType> csvFieldTypes = Lists.newArrayListWithCapacity(fieldTypes.size());
        for (SqlTypeName sqlTypeName : fieldTypes) {
            switch (sqlTypeName) {
                case VARCHAR:
                    csvFieldTypes.add(CsvFieldType.STRING);
                    break;
                case INTEGER:
                    csvFieldTypes.add(CsvFieldType.INT);
                    break;
                case DECIMAL:
                    csvFieldTypes.add(CsvFieldType.DOUBLE);
                    break;
                default:
                    throw new RuntimeException("Unsupported type " + sqlTypeName + " in csv");
            }
        }
        return csvFieldTypes;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        if (rowType == null) {
            ArrayList<RelDataTypeField> fields = Lists.newArrayListWithCapacity(fieldNames.size());

            for (int i = 0; i < fieldNames.size(); i++) {
                RelDataType fieldType = relDataTypeFactory.createSqlType(fieldTypes.get(i));
                RelDataTypeFieldImpl field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
                fields.add(field);
            }
            rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields);
        }
        return rowType;
    }

    public static Builder newBuilder(String tableName) {
        return new Builder(tableName);
    }

    public static final class Builder {
        private final String tableName;
        private String filePath;
        private final List<String> fieldNames = Lists.newArrayList();
        private final List<SqlTypeName> fielsTypes = Lists.newArrayList();
        private long rowCount;

        public Builder(String tableName) {
            if (StringUtils.isEmpty(tableName)) {
                throw new IllegalArgumentException("Table name cannot be null or empty");
            }
            this.tableName = tableName;
        }

        public Builder addField(String name, SqlTypeName typeName) {
            if (StringUtils.isEmpty(name)) {
                throw new IllegalArgumentException("Field name cannot be null or empty");
            }
            if (fieldNames.contains(name)) {
                throw new IllegalArgumentException("Field already defined: " + name);
            }
            fieldNames.add(name);
            fielsTypes.add(typeName);
            return this;
        }

        public Builder withFilePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder withRowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public SimpleTable build() {
            if (fieldNames.isEmpty()) {
                throw new IllegalStateException("Table must have at least one field");
            }
            if (rowCount == 0L) {
                throw new IllegalStateException("Table must have positive row count");
            }
            return new SimpleTable(tableName, filePath, fieldNames, fielsTypes, new SimpleTableStatistic(rowCount));
        }

    }
}
