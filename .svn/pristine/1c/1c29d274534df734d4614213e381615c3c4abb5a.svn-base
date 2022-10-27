package com.eurlanda.datashire.engine.mr.phoenix.parse;

import com.eurlanda.datashire.common.util.ConstantsUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.schema.types.*;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 16/2/24.
 */
public class FilterListParseNodeVisitor implements ParseNodeVisitor<Filter> {
    @Override public List<Filter> newElementList(int size) {
        return new ArrayList<Filter>(2);
    }

    @Override public void addElement(List<Filter> a, Filter element) {
        a.add(element);
    }

    @Override public boolean visitEnter(LikeParseNode node) throws SQLException {
        return true;
    }

    @Override public Filter visitLeave(LikeParseNode node, List<Filter> l)
            throws SQLException {
        // 取出column的那一列
        ColumnParseNode cnode = (ColumnParseNode)node.getLHS();

        LiteralParseNode literalParseNode = (LiteralParseNode)node.getRHS();
        String exp = literalParseNode.getValue().toString();

        // 为字符,则使用binary
        Filter filter = null;
        // 需要判断是否为:key,此时使用行健来使用
        if(cnode.getName().equals(ConstantsUtil.HBASE_ROW_KEY) && StringUtils.isEmpty(cnode.getTableName())) {
            filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(exp));
        } else {
            byte[] family = Bytes.toBytes(cnode.getTableName());
            byte[] qualifier = Bytes.toBytes(cnode.getName());
            filter = new SingleColumnValueFilter(family, qualifier,
                    CompareFilter.CompareOp.EQUAL, new RegexStringComparator(exp));
        }

        return filter;
    }

    @Override public boolean visitEnter(AndParseNode node) throws SQLException {
        return true;
    }

    @Override public Filter visitLeave(AndParseNode node, List<Filter> l)
            throws SQLException {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        flatFilterList(filterList, l);
        return filterList;
    }

    @Override public boolean visitEnter(OrParseNode node) throws SQLException {
        return true;
    }

    @Override public Filter visitLeave(OrParseNode node, List<Filter> l)
            throws SQLException {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        flatFilterList(filterList, l);
        return filterList;
    }

    @Override public boolean visitEnter(FunctionParseNode node) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override public Filter visitLeave(FunctionParseNode node, List<Filter> l)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        // 比较节点
        return true;
    }

    @Override public Filter visitLeave(ComparisonParseNode node, List<Filter> l)
            throws SQLException {
        ParseNode lnode = node.getLHS();
        ParseNode rnode = node.getRHS();
        // 判断列在左边还是右边
        boolean isLeft = isLeft(lnode, rnode);
        // 取出column的那一列
        ParseNode columnNode = (isLeft ? lnode : rnode);
        ColumnParseNode cnode = null;
        if(columnNode instanceof ColumnParseNode){
            cnode = (ColumnParseNode)(columnNode);
        }
        // value 列
        ParseNode vnode = isLeft ? rnode : lnode;

        LiteralParseNode literalParseNode = (LiteralParseNode)vnode;
//        byte[] bytes = Bytes.toBytes(literalParseNode.getValue().toString());

        // 根据数据类型来决定
        PDataType pdt = literalParseNode.getType();

        byte[] bytes = null;
        Object v = literalParseNode.getValue();
        if(pdt instanceof PInteger) {
            bytes = Bytes.toBytes((Integer) v);
        } else if(pdt instanceof PLong) {
            bytes = Bytes.toBytes((Long) v);
        } else if(pdt instanceof PVarchar) {
            bytes = Bytes.toBytes((String) v);
        } else if(pdt instanceof PFloat) {
            bytes = Bytes.toBytes((Float) v);
        } else if(pdt instanceof PDouble) {
            bytes = Bytes.toBytes((Double) v);
        } else if(pdt instanceof PDecimal) {
            bytes = Bytes.toBytes((BigDecimal) v);
        } else {
            throw new RuntimeException("不能解析该类型, FilterListParseNodeVisitor:" + pdt.toString());
        }

//        byte[] bytes = pdt.toBytes(literalParseNode.getValue());

        // 为字符,则使用binary
        Filter filter = null;
        // 需要判断是否为:key,此时使用行健来使用
        if(cnode!=null) {
            if (cnode.getName().equals(ConstantsUtil.HBASE_ROW_KEY) && StringUtils.isEmpty(cnode.getTableName())) {
                filter = new RowFilter(isLeft ? node.getFilterOp() : node.getInvertFilterOp(), new BinaryComparator(bytes));
            } else {
                byte[] family = Bytes.toBytes(cnode.getTableName());
                String name = cnode.getName();
                byte[] qualifier = Bytes.toBytes(name);
                filter = new SingleColumnValueFilter(family, qualifier,
                        isLeft ? node.getFilterOp() : node.getInvertFilterOp(),
                        new BinaryComparator(bytes));
            }
        }
        return filter;
    }

    private boolean isColumn(ParseNode node) {
        return node instanceof ColumnParseNode;
    }

    private boolean isLeft(ParseNode lnode, ParseNode rnode) {
        if(isColumn(lnode) && !isColumn(rnode)) {
            return true;
        } else {
            return false;
        }
    }

    @Override public boolean visitEnter(CaseParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(CaseParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(CompoundParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(CompoundParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(AddParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(AddParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(MultiplyParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(ModulusParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(ModulusParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(DivideParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(DivideParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(SubtractParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(SubtractParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(NotParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(NotParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(ExistsParseNode existsParseNode) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(ExistsParseNode existsParseNode, List<Filter> list)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(InListParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(InListParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(InParseNode inParseNode) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(InParseNode inParseNode, List<Filter> list)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(IsNullParseNode node) throws SQLException {
        return true;
    }

    @Override public Filter visitLeave(IsNullParseNode node, List<Filter> l)
            throws SQLException {
        ParseNode pn = node.getChildren().get(0);
        if(pn instanceof ColumnParseNode) {
            ColumnParseNode cpn = (ColumnParseNode)pn;
            String columnName = cpn.getName();
            if(ConstantsUtil.HBASE_ROW_KEY.equals(columnName) && StringUtils.isEmpty(cpn.getTableName())) {
                throw new RuntimeException();
            } else {
                SingleColumnValueFilter filter = new SingleColumnValueFilter(
                        Bytes.toBytes(cpn.getTableName()), Bytes.toBytes(cpn.getName()),
                        CompareFilter.CompareOp.NO_OP, new NullComparator());
                return filter;
            }
        } else {
            throw new RuntimeException();
        }
    }

    @Override public Filter visit(ColumnParseNode node) throws SQLException {
        return null;
    }

    @Override public Filter visit(LiteralParseNode node) throws SQLException {
        return null;
    }

    @Override public Filter visit(BindParseNode node) throws SQLException {
        return null;
    }

    @Override public Filter visit(WildcardParseNode node) throws SQLException {
        return null;
    }

    @Override public Filter visit(TableWildcardParseNode node) throws SQLException {
        return null;
    }

    @Override public Filter visit(FamilyWildcardParseNode node) throws SQLException {
        return null;
    }

    @Override public Filter visit(SubqueryParseNode subqueryParseNode) throws SQLException {
        return null;
    }

    @Override public Filter visit(ParseNode node) throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(StringConcatParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(StringConcatParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(BetweenParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(BetweenParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(CastParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(CastParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(RowValueConstructorParseNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(RowValueConstructorParseNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(ArrayConstructorNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(ArrayConstructorNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public Filter visit(SequenceValueParseNode node) throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(ArrayAllComparisonNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(ArrayAllComparisonNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(ArrayAnyComparisonNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(ArrayAnyComparisonNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    @Override public boolean visitEnter(ArrayElemRefNode node) throws SQLException {
        return false;
    }

    @Override public Filter visitLeave(ArrayElemRefNode node, List<Filter> l)
            throws SQLException {
        return null;
    }

    private void flatFilterList(FilterList fl, List<Filter> list) {
        for(Filter f : list) {
            if(f!=null) {
                fl.addFilter(f);
            }
        }
    }
}
