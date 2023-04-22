package com.hjl;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * @Description
 * @Author jiale.he
 * @Date 2022-08-09 13:07 周二
 */
public class SimpleTableStatistic implements Statistic {
    private final long rowCount;

    public SimpleTableStatistic(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public @Nullable Double getRowCount() {
        return (double) this.rowCount;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

//    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return Collections.emptyList();
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        return Collections.emptyList();
    }

    @Override
    public @Nullable List<RelCollation> getCollations() {
        return Collections.emptyList();
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        return RelDistributionTraitDef.INSTANCE.getDefault();
    }
}
