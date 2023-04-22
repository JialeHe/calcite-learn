package com.hjl;

import com.hjl.utils.CalciteUtils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.LinkedHashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws SqlParseException {

        String userPath = "/Users/jiale.he/IdeaProjects/calcite-learn/src/main/resources/users.csv";
        String ordersPath = "/Users/jiale.he/IdeaProjects/calcite-learn/src/main/resources/orders.csv";

        SimpleTable userTable = SimpleTable.newBuilder("users")
                .addField("id", SqlTypeName.VARCHAR)
                .addField("name", SqlTypeName.VARCHAR)
                .addField("age", SqlTypeName.INTEGER)
                .withFilePath(userPath)
                .withRowCount(10)
                .build();
        SimpleTable ordersTable = SimpleTable.newBuilder("orders")
                .addField("id", SqlTypeName.VARCHAR)
                .addField("user_id", SqlTypeName.VARCHAR)
                .addField("goods", SqlTypeName.VARCHAR)
                .addField("price", SqlTypeName.DECIMAL)
                .withFilePath(ordersPath)
                .withRowCount(10)
                .build();

        SimpleSchema schema = SimpleSchema.newBuilder("default")
                .addTable(userTable)
                .addTable(ordersTable)
                .build();

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getSchemaName(), schema);

//        String sql = "SELECT u.id, name, age, sum(price)" +
//                "FROM users AS u join orders AS o ON u.id = o.user_id " +
//                "WHERE age >= 20 AND age <= 30 " +
//                "GROUP BY u.id, name, age " +
//                "ORDER BY u.id";

        String sql = "SELECT u.id, name, age, SUM(price), SUM(case when age >= 20 and age <= 25 then 25 else 30 end) as flag" +
                "FROM users AS u" +
                "INNER JOIN orders AS o ON u.id = o.user_id" +
                "WHERE age >= 20 AND age <= 30" +
                "GROUP BY u.id, name, age" +
                "ORDER BY u.id";


        Optimizer optimizer = Optimizer.create(schema);

        // 1. SQL Parser: SQL string --> SqlNode
        SqlNode sqlNode = optimizer.parse(sql);
        CalciteUtils.print("SQL Parser: ", sqlNode.toString());

        // 2. SQL Validate: SqlNode --> SqlNode
        SqlNode validateSqlNode = optimizer.validate(sqlNode);

        // 3. SQL Convert: SqlNode --> RelNode
        RelNode relNode = optimizer.convert(validateSqlNode);
        CalciteUtils.print("SQL Convert: ", relNode.explain());

        // 4. SQL Optimize: RelNode --> RelNode
        RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                // 谓词下推 过滤条件下推到 JOIN 之前
                CoreRules.FILTER_INTO_JOIN,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE
        );
        RelNode optimizerRelTree = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(EnumerableConvention.INSTANCE),
                rules);
        CalciteUtils.print("OPTIMIZE RESULT: ", optimizerRelTree.explain());

        // 5. SQL execute: RelNode --> execute code
        EnumerableRel enumerable = (EnumerableRel) optimizerRelTree;
        Map<String, Object> internalParameters = new LinkedHashMap<>();
        EnumerableRel.Prefer prefer = EnumerableRel.Prefer.ARRAY;
        Bindable bindable = EnumerableInterpretable.toBindable(internalParameters,
                null, enumerable, prefer);
        Enumerable bind = bindable.bind(new SimpleDataContext(rootSchema.plus()));
        Enumerator enumerator = bind.enumerator();
        while (enumerator.moveNext()) {
            Object current = enumerator.current();
            Object[] values = (Object[]) current;
            StringBuilder sb = new StringBuilder();
            for (Object value : values) {
                sb.append(value).append(",");
            }
            sb.setLength(sb.length() - 1);
            System.out.println(sb);
        }


    }
}