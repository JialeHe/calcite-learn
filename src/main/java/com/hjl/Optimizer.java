package com.hjl;

import com.google.common.collect.Lists;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.Properties;

/**
 * @Description
 * @Author jiale.he
 * @Date 2022-08-09 14:00 周二
 */
public class Optimizer {

    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;
    private final VolcanoPlanner planner;

    public Optimizer(CalciteConnectionConfig config, SqlValidator validator,
                     SqlToRelConverter converter, VolcanoPlanner planner) {
        this.config = config;
        this.validator = validator;
        this.converter = converter;
        this.planner = planner;
    }

    public static Optimizer create(SimpleSchema schema) {
        Properties prop = new Properties();
        prop.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        prop.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        prop.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfigImpl config = new CalciteConnectionConfigImpl(prop);

        // create root schema
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getSchemaName(), schema);

        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();

        // create catalog reader, needed by SqlValidator
        // 创建CatalogReader, 用于指示如何读取Schema信息
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Lists.newArrayList(schema.getSchemaName()),
                typeFactory,
                config);

        // create SqlValidator
        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);
        SqlValidatorWithHints validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                validatorConfig);

        // create VolcanoPlanner, needed by SqlToRelConverter and optimizer
        // 创建 VolcanoPlanner, VolcanoPlanner 在后面的优化中还需要用到
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        // create SqlToRelConverter
        RelOptCluster cluster = RelOptCluster.create(volcanoPlanner, new RexBuilder(typeFactory));
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);
        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        return new Optimizer(config, validator, converter, volcanoPlanner);
    }

    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser.Config parserConfig = SqlParser.config()
                .withQuotedCasing(config.quotedCasing())
                .withUnquotedCasing(config.unquotedCasing())
                .withQuoting(config.quoting())
                .withConformance(config.conformance())
                .withCaseSensitive(config.caseSensitive())
                .withParserFactory(SqlParserImpl.FACTORY);
        return SqlParser.create(sql, parserConfig).parseStmt();
    }

    public SqlNode validate(SqlNode sqlNode) {
        return validator.validate(sqlNode);
    }

    public RelNode convert(SqlNode sqlNode) {
        RelRoot relRoot = converter.convertQuery(sqlNode, false, true);
        return relRoot.rel;
    }

    public RelNode optimize(RelNode relNode, RelTraitSet relTraitSet, RuleSet rules) {
        Program program = Programs.of(RuleSets.ofList(rules));

        return program.run(planner, relNode, relTraitSet, Lists.newArrayList(), Lists.newArrayList());
    }


}
