package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.adapter.datatype.TypeMapping;
import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.common.util.HbaseUtil;
import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.exception.TargetSquidNotPersistException;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.DateUtil;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.engine.util.cool.JMap;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.*;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.Constants;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;

/**
 * 基础编译器。
 *
 * @author Gene
 */
public abstract class AbsTSquidBuilder implements TSquidBuilder {
    private boolean removeNoUseKeys = false;
    protected BuilderContext ctx;
    protected Squid currentSquid;
    protected List<TSquid> currentBuildedSquids = new ArrayList<>();
    private static Logger logger = LoggerFactory.getLogger(AbsTSquidBuilder.class);

    public AbsTSquidBuilder(BuilderContext ctx, Squid currentSquid) {
        this.ctx = ctx;
        this.currentSquid = currentSquid;
    }

    // 中间缓存数据
    // 开始trans
    List<Transformation> beginTrs;
    // id -> trans
    Map<Integer, Transformation> id2trans;


    /**
     * 取当前squid翻译过程中的最后一个squid.
     *
     * @return 如果没有，返回 null
     */
    public TSquid getCurLastTSqud() {

        if (this.currentBuildedSquids.size() > 0) {
            for (int i = this.currentBuildedSquids.size() - 1; i >= 0; i--) {
                TSquid ts = this.currentBuildedSquids.get(i);

                return ts;
            }
        }
        return null;
    }

    /**
     * 取一个squid翻译之后的DataFallSquid.
     *
     * @return 如果没有，返回 null
     */
    public TSquid getCurDataFallSquid() {

        for (TSquid ts : currentBuildedSquids) {
            if (ts.getType() == TSquidType.DATA_FALL_SQUID)
                return ts;
        }
        return null;

    }

    /**
     * 取连接到当前squid的所有前置squid.
     *
     * @return
     */
    protected List<Squid> getPrevSquids() {
        return ctx.getPrevSquids(this.currentSquid);
    }

    /**
     * 生成一个递减的key,key为负数。
     *
     * @return
     */
    protected Integer genOutKey() {
        return ctx.genOutKey();
    }

    /**
     * 根据指定的id获取一个extractSquid中的transformation
     *
     * @param id
     * @param squid
     * @return
     */
    protected Transformation getExtractSquidTransformationById(int id, DataSquid squid) {
        List<Transformation> ts = squid.getTransformations();
        for (Transformation t : ts) {
            if (t.getId() == id) {
                return t;
            }
        }
        return null;
    }

    /**
     * 根据指定的id获取一个extractSquid中的transformation
     *
     * @param id
     * @param squid
     * @return
     */
    protected Transformation getExtractSquidTransformationById(int id, DocExtractSquid squid) {
        List<Transformation> ts = squid.getTransformations();
        for (Transformation t : ts) {
            if (t.getId() == id) {
                return t;
            }
        }
        return null;
    }

    /**
     * 从squid 的column id ,找到ReferenceColumn.
     *
     * @param colId
     * @return
     */
    public ReferenceColumn getRefColByColumnId(DataSquid ds, int colId) {

        for (Transformation tf : ds.getTransformations()) {
            if (tf.getColumn_id() == colId) {        //  找到transformation.
                for (TransformationLink lk : ds.getTransformationLinks()) { // 找到 link
                    if (tf.getId() == lk.getTo_transformation_id()) {    // 找到 link
                        for (Transformation tar : ds.getTransformations()) {
                            if (tar.getId() == lk.getFrom_transformation_id()) {
                                for (ReferenceColumn rc : ds.getSourceColumns()) {        // 找到 rc.
                                    if (rc.getColumn_id() == tar.getColumn_id()) {
                                        return rc;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    public Column getColumnById(DataSquid ds, int columnId) {
        for (Column c : ds.getColumns()) {
            if (c.getId() == columnId) {
                return c;
            }
        }
        throw new RuntimeException("squid[" + ds.getName() + "]中不存在id为" + columnId + "的column");
    }

    /**
     * 从squid 的ReferenceColumn id ,找到 column
     *
     * @param rcolId
     * @return
     */
    public Column getColumnByRefColumnId(DataSquid ds, int rcolId) {
        return getColumnByRefColumnId(ds.getTransformations(),
                ds.getTransformationLinks(), ds.getColumns(), rcolId);
    }

    public Column getColumnByRefColumnId(DocExtractSquid ds, int rcolId) {
        return getColumnByRefColumnId(ds.getTransformations(),
                ds.getTransformationLinks(), ds.getColumns(), rcolId);
    }

    protected Column getColumnByRefColumnId(List<Transformation> transformationList,
                                            List<TransformationLink> transformationLinks, List<Column> columns,
                                            int rcolId) {
        for (Transformation tf : transformationList) {
            if (tf.getColumn_id() == rcolId) {        //  找到transformation.
                for (TransformationLink lk : transformationLinks) { // 找到 link
                    if (tf.getId() == lk.getFrom_transformation_id()) {    // 找到 link
                        for (Transformation tar : transformationList) {
                            if (tar.getId() == lk.getTo_transformation_id()) {
                                for (Column rc : columns) {        // 找到 rc.
                                    if (rc.getId() == tar.getColumn_id()) {
                                        return rc;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * 抽取Tcolumns 中得主键，如果没有主键，则使用系统默认主键
     *
     * @param cols
     * @return
     */
    public Set<TColumn> buildDataSquidPKTColumns(Set<TColumn> cols) {
        Set<TColumn> columnMap = new HashSet<>();
        for (TColumn t : cols) {
            if (t.isPrimaryKey()) {
                TColumn tc = new TColumn();
                tc.setName(t.getName());
                tc.setId(-1);
                tc.setPrimaryKey(true);
                columnMap.add(tc);
            }
        }
        if (columnMap.size() == 0) {
            TColumn tc = new TColumn();
            tc.setName(HbaseUtil.DEFAULT_PK_NAME);
            tc.setId(-1);
            tc.setPrimaryKey(true);
            columnMap.add(tc);
        }
        return columnMap;
    }

    /**
     * 编译dataSquid的Column 为 TColumn.  用在ExtractSquid.
     * 将该dataSquid的主键获取出来，如果没有主键，则使用系统默认主键
     *
     * @param wes extractSquid.
     * @return
     */
    public Set<TColumn> buildDataSquidPKTColumns(DataSquid wes) {
        Set<TColumn> columnMap = new HashSet<>();
        List<ReferenceColumn> rcList = wes.getSourceColumns();
        for (ReferenceColumn rc : rcList) {
            if (rc.isIsPK()) {
                TColumn tc = new TColumn();
                tc.setName(rc.getName());
                tc.setId(-1);
                tc.setPrimaryKey(true);
                columnMap.add(tc);
            }
        }
        if (columnMap.size() == 0) {
            TColumn tc = new TColumn();
            tc.setName(HbaseUtil.DEFAULT_PK_NAME);
            tc.setId(-1);
            tc.setPrimaryKey(true);
            columnMap.add(tc);
        }
        return columnMap;
    }

    /**
     * 编译dataSquid的Column 为 TColumn.  用在ExtractSquid.
     * 用于 数据库抽取
     *
     * @param wes extractSquid.
     *            todo 重构实现逻辑,通过column 来寻找到输出列, 复用 getReferenceColumnByColumnId() 防范
     * @return
     */
    public Set<TColumn> buildDataSquidTColumns(DataSquid wes) {
        Set<TColumn> columnMap = new HashSet<>();
        List<ReferenceColumn> rcList = wes.getSourceColumns();
        // 根据transformationLink 过滤sourceColumn
        Integer dbType = null;

        Squid ds = this.ctx.getPrevSquids(currentSquid).get(0);
        if (ds instanceof DbSquid) {
            dbType = ((DbSquid) ds).getDb_type();
        } else if (ds instanceof WebSquid) {
            dbType = DataBaseType.HBASE_PHOENIX.value();
        } else if (ds instanceof WeiboSquid) {
            dbType = DataBaseType.HBASE_PHOENIX.value();
        }
        // 保存所有有连线的column Id
        Set<Integer> columnKeys = new HashSet<>();
        for (TransformationLink tl : wes.getTransformationLinks()) {// 因为需要getColumnIdByColumn用getFrom_transformation_id  // 只能得到局部

            Transformation virtTransformation = getExtractSquidTransformationById(tl.getFrom_transformation_id(), wes);
            int vtColumnId = virtTransformation.getColumn_id();
            // toTransformation
            Transformation toTran = getExtractSquidTransformationById(tl.getTo_transformation_id(), wes);
            int columnId = toTran.getColumn_id();

            for (ReferenceColumn rColumn : rcList) {
                if (rColumn.getColumn_id() == vtColumnId) {
                    TColumn c = new TColumn();

                    c.setDbBaseDatatype(DbBaseDatatype.parse(rColumn.getData_type()));
//					Column col = getColumnByRefColumnId((DataSquid)this.currentSquid,rColumn.getColumn_id());
                    Column col = getColumnById((DataSquid) this.currentSquid, columnId);
                    TDataType type = TDataType.sysType2TDataType(col.getData_type());
                    if (type == null) {
                        throw new RuntimeException(String.format("Squid[%s] 转换ReferenceColumn[%s] type[%s]出错，数据类型为null", wes.getName(), rColumn.getName(), rColumn.getData_type()));
                    } else {
                        c.setData_type(type);
                    }
                    // 获取该虚拟转换对应到的key
                    c.setId(col.getId());
                    // 添加column id 到 columnKeys中
                    columnKeys.add(col.getId());
                    c.setName(rColumn.getName());
                    c.setBusinessKey(rColumn.getIs_Business_Key() == 1 ? true : false);
                    c.setCdc(rColumn.getCdc());
                    SourceColumn sc = new SourceColumn();
                    try {
                        BeanUtils.copyProperties(sc, rColumn);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    if (!(wes instanceof WebLogExtractSquid || wes instanceof XmlExtractSquid || wes instanceof WeiBoExtractSquid)) {
                        TypeMapping tm = DataTypeConvertor.getInTypeMappingBySourceColumn(dbType, sc);
                        if (tm != null) {
                            if("BFILE".equals(tm.getDbDataType())){
                                c.setJdbcDataType(TDataType.str2TDataType("byte[]".toLowerCase()));
                            } else {
                                c.setJdbcDataType(TDataType.str2TDataType(tm.getJavaType().toLowerCase()));
                            }
                        }
                    }
                    c.setLength(rColumn.getLength());
                    c.setNullable(rColumn.isNullable());
                    c.setPrecision(rColumn.getPrecision());
                    c.setPrimaryKey(rColumn.isIsPK());
                    c.setScale(rColumn.getScale());
                    columnMap.add(c);
                    break;
                }
            }
        }
        // 寻找没有线的 extraction_date
        for (Column col : wes.getColumns()) {
            if ((!columnKeys.contains(col.getId())) && Constants.DEFAULT_EXTRACT_COLUMN_NAME.equalsIgnoreCase(col.getName())) {
                TColumn e = new TColumn();
                e.setSourceColumn(false);
                e.setId(col.getId());
                e.setName(col.getName());
                e.setData_type(TDataType.sysType2TDataType(col.getData_type()));
                e.setExtractTime(DateUtil.nowTimestamp());
                columnMap.add(e);
                break;
            }
        }
        return columnMap;
    }

    public String getExtractSquidTableName(int id, int sourceTableId) {
        if (sourceTableId == 0) {
            throw new RuntimeException("未找到extractSquid[" + id + "]引用的sourceTable[" + sourceTableId + "]");
        }
        String sql = "select table_name from ds_source_table where id=" + sourceTableId;
        Map<String, Object> ret = ConstantUtil.getJdbcTemplate().queryForMap(sql);

        return (String) ret.get("table_name");
    }

    /**
     * 获取抽取squid中,分片列的sourceColumn
     * @param id extractsquid id
     * @param sourceTableId
     * @param columnName  分片列名称
     * @return
     */
    public SourceColumn getExtractSquidSourceColumns(int id, int sourceTableId, String columnName) {
        if (sourceTableId == 0) {
            throw new RuntimeException("未找到extractSquid[" + id + "]引用的sourceTable[" + sourceTableId + "]");
        }
        String sql = "select * from ds_source_column where SOURCE_TABLE_ID=" + sourceTableId
                + " and name='" + columnName + "'";
        SourceColumn column = null;
        try {
            List<SourceColumn> columns = HyperSQLManager.query2List(ConstantUtil.getJdbcTemplate().getDataSource().getConnection(),true,sql,null,SourceColumn.class);
            if(columns!=null && columns.size()>0){
                column = columns.get(0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return column;
    }

    /**
     * 通过column 寻找到对应的referenceColumn
     * 场景为 referenceColumn 通过一根线 连接到 Column上
     *
     * @param columnId
     * @param linkList
     * @param transformationList
     * @param rcList
     * @return
     */
    protected ReferenceColumn getReferenceColumnByColumnId(int columnId, List<TransformationLink> linkList, List<Transformation> transformationList, List<ReferenceColumn> rcList) {
        for (TransformationLink link : linkList) {
            Transformation toTran = getTransformationById(transformationList, link.getTo_transformation_id());
            if (toTran != null && columnId == toTran.getColumn_id()) {
                Transformation fromTran = getTransformationById(transformationList, link.getFrom_transformation_id());

                for (ReferenceColumn rc : rcList) {
                    // 匹配 到对应的referenceColumn
                    if (rc.getColumn_id() == fromTran.getColumn_id()) {
                        return rc;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 通过 transId 在 transformationList 中找到对应的 Transforamtion
     *
     * @param transformationList
     * @param transId
     * @return
     */
    protected Transformation getTransformationById(List<Transformation> transformationList, int transId) {
        for (Transformation t : transformationList) {
            if (t.getId() == transId) {
                return t;
            }
        }
        throw new RuntimeException("没有存在该transformation");
    }

    public Set<TColumn> buildDataSquidTColumns(DocExtractSquid wes) {
        Set<TColumn> columnMap = new HashSet<>();


        List<Column> rcList = wes.getColumns();
        // 根据transformationLink 过滤sourceColumn
        for (TransformationLink tl : wes.getTransformationLinks()) {// 因为需要getColumnIdByColumn用getFrom_transformation_id  // 只能得到局部

            Transformation virtTransformation = getExtractSquidTransformationById(tl.getTo_transformation_id(), wes);
            int vtColumnId = virtTransformation.getColumn_id();
            for (Column rColumn : rcList) {
                if (rColumn.getId() == vtColumnId) {
                    TColumn c = new TColumn();
                    c.setData_type(TDataType.sysType2TDataType(rColumn.getData_type()));
                    // 获取该虚拟转换对应到的key
                    c.setId(vtColumnId);
                    c.setName(rColumn.getName());
                    c.setBusinessKey(rColumn.getIs_Business_Key() == 1 ? true : false);
                    columnMap.add(c);
                    break;
                }
            }
        }
        for (Column col : wes.getColumns()) {
            if (Constants.DEFAULT_EXTRACT_COLUMN_NAME.equals(col.getName())) {
                TColumn e = new TColumn();
                e.setSourceColumn(false);
                e.setId(col.getId());
                e.setName(e.getName());
                columnMap.add(e);
                break;
            }
        }
        return columnMap;
    }

    public List<TSquid> translate(Squid squid) {
        return doTranslate(squid);
    }

    /**
     * 处理，处理模式。
     *
     * @param stageSquid
     * @return
     */
    protected TSquid doTranslateProcessMode(DataSquid stageSquid) {
        // stage的输出模式，是增量输出，还是全量输出。0增量 1全量
        if (stageSquid.getProcess_mode() == 1 && stageSquid.isIs_persisted()) {
            TDataFallSquid lastSquid = (TDataFallSquid) this.getCurDataFallSquid(); // 最后一个squid.
            TDatabaseSquid ds = new TDatabaseSquid();
            ds.setDataSource(lastSquid.getDataSource());
            try {
                Set<TColumn> columnSet = new HashSet<>();
                for (TColumn tc : lastSquid.getColumnSet()) {
                    columnSet.add((TColumn) BeanUtils.cloneBean(tc));
                }
                ds.setColumnSet(columnSet);
            } catch (Exception e) {
                throw new RuntimeException("翻译异常，克隆column失败");
            }
            // 对hbase 需要表主键，将主键column id设为-1，hbase需要该主键来分页
            if (ds.getDataSource().getType() == DataBaseType.HBASE_PHOENIX) {
                ds.getColumnSet().addAll(buildDataSquidPKTColumns(ds.getColumnSet()));
            }

            TInnerExtractSquid tes = new TInnerExtractSquid();
            tes.setPreTSquid(lastSquid);
            tes.setSquidId(stageSquid.getId());
            tes.settDatabaseSquid(ds);
            tes.setName(stageSquid.getName());
            return tes;
        }
        return null;
    }

    /**
     * 翻译落地squid.
     *
     * @param isPersisted
     * @param columns
     * @param destinationSquidId
     * @param id
     * @param name
     * @param truncateExistingDataFlag
     * @param tableName
     * @param cdc
     * @return
     */
    public TSquid doTranslateDataFall(boolean isPersisted, List<Column> columns,
                                      int destinationSquidId,
                                      int id,
                                      String name,
                                      int truncateExistingDataFlag,
                                      String tableName, int cdc) {
        if (isPersisted) { // 设置为落地
            // 默认表结构已经生成了, 表名为： sf_implied.squidFlowId + _ + squidId
            // 1. 获取所有的columns，
            // 2. 根据column信息，翻译成 TColumn
            // 3. TDataSource信息
            Set<TColumn> tColumnMap = new HashSet<>();
//			List<Column> columns = stageSquid.getColumns();
            for (Column c : columns) {
                TColumn tColumn = new TColumn();
                tColumn.setId(c.getId());
                tColumn.setName(c.getName());
                tColumn.setLength(c.getLength());
                tColumn.setPrecision(c.getPrecision());
                tColumn.setPrimaryKey(c.isIsPK());
                tColumn.setData_type(TDataType.sysType2TDataType(c.getData_type()));
                tColumn.setNullable(c.isNullable());
                tColumn.setCdc(c.getCdc() == 1 ? 2 : 1);        //  后台的是=2，否=1
                tColumn.setBusinessKey(c.getIs_Business_Key() == 1);
                tColumnMap.add(tColumn);
            }
            if (destinationSquidId == 0) { // 设置落地但没有指定落地库。隐式落地。
                throw new TargetSquidNotPersistException(name);
//				//落地到Hbase
//				TDataSource tDataSource = new TDataSource();
//				tDataSource.setType(DataBaseType.HBASE_PHOENIX);
//				// 隐式落地,改用设置的表名
//				//tDataSource.setTableName(HbaseUtil.genImpliedTableName(this.ctx.getRepositoryId(), stageSquid.getSquidflow_id(), stageSquid.getId()));
//				tDataSource.setTableName(tableName);
//				tDataSource.setHost(ConfigurationUtil.getInnerHbaseHost() + ":" + ConfigurationUtil.getInnerHbasePort());
////				tDataSource.setPort(ConfigurationUtil.getInnerHbasePort());
//				tDataSource.setCDC(cdc == 1 ? true : false);
//				// TDataSource tDataSource = new TDataSource("192.168.137.2",
//				// 3306,
//				// "squidflowtest", "root", "root", stageSquid.getSquidflow_id()
//				// +
//				// "_" + stageSquid.getId(), DataBaseType.MYSQL);
//				TDataFallSquid tDataFallSquid = new TDataFallSquid();
//				tDataFallSquid.setTruncateExistingData(truncateExistingDataFlag == 0 ? false : true);
//				tDataFallSquid.setPreviousSquid(this.getCurLastTSqud());
//				tDataFallSquid.setDataSource(tDataSource);
//				tDataFallSquid.setSquidId(id);
//				tDataFallSquid.setType(TSquidType.DATA_FALL_SQUID);
//				tDataFallSquid.setColumnSet(tColumnMap);
//				tDataFallSquid.setName(name);
//				return tDataFallSquid;


            } else { // 指定地点的落地。
                int squidId = destinationSquidId;
                Squid destSquid = ctx.getSquidById(squidId);
                SquidTypeEnum squidTypeEnum = SquidTypeEnum.parse(destSquid.getSquid_type());
                if (squidTypeEnum == SquidTypeEnum.DBSOURCE
                        || squidTypeEnum == SquidTypeEnum.CLOUDDB
                        || squidTypeEnum==SquidTypeEnum.TRAININGDBSQUID) {
                    DbSquid dbSquid = (DbSquid) destSquid;

                    TDataFallSquid tdf = new TDataFallSquid();
                    tdf.setTruncateExistingData(truncateExistingDataFlag == 0 ? false : true);
                    TDataSource tDataSource = new TDataSource();
                    tDataSource.setCDC(cdc == 1);
                    tDataSource.setDbName(dbSquid.getDb_name());
                    tDataSource.setUserName(dbSquid.getUser_name());
                    tDataSource.setPassword(dbSquid.getPassword());
                    tDataSource.setType(DataBaseType.parse(dbSquid.getDb_type()));
                    tDataSource.setTableName(tableName);
                    tDataSource.setHost(dbSquid.getHost());
                    tDataSource.setPort(dbSquid.getPort());

                    tdf.setPreviousSquid(this.getCurLastTSqud());
                    tdf.setDataSource(tDataSource);
                    tdf.setSquidId(id);
                    tdf.setType(TSquidType.DATA_FALL_SQUID);
                    tdf.setName(name);
                    tdf.setColumnSet(tColumnMap);
                    return tdf;
                } else if (squidTypeEnum == SquidTypeEnum.MONGODB) {
                    NOSQLConnectionSquid nosqlConnectionSquid = (NOSQLConnectionSquid) destSquid;
                    NoSQLDataBaseType dataBaseType = NoSQLDataBaseType.parse(nosqlConnectionSquid.getDb_type());
                    switch (dataBaseType) {
                        case MONGODB:
                            TMongoDataFallSquid tdf = new TMongoDataFallSquid();
                            tdf.setTruncateExistingData(truncateExistingDataFlag == 0 ? false : true);

                            TNoSQLDataSource tDataSource = new TNoSQLDataSource();
                            tDataSource.setCDC(cdc == 1);
                            tDataSource.setDbName(nosqlConnectionSquid.getDb_name());
                            tDataSource.setUserName(nosqlConnectionSquid.getUser_name());
                            tDataSource.setPassword(nosqlConnectionSquid.getPassword());
                            tDataSource.setType(dataBaseType);
                            tDataSource.setTableName(tableName);
                            tDataSource.setHost(nosqlConnectionSquid.getHost());
                            tDataSource.setPort(nosqlConnectionSquid.getPort());

                            tdf.setPreviousSquid(this.getCurLastTSqud());
                            tdf.setDataSource(tDataSource);
                            tdf.setSquidId(id);
                            tdf.setName(name);
                            tdf.setColumnSet(tColumnMap);
                            return tdf;
                        default:
                            throw new RuntimeException("不能处理该NOSQL数据库类型" + dataBaseType);
                    }
                }
            }
        }
        return null;
    }

    public abstract List<TSquid> doTranslate(Squid squid);

    /**
     * 落地squid.
     *
     * @param stageSquid
     * @return
     */
    public TSquid doTranslateDataFall(DocExtractSquid stageSquid) {
        return doTranslateDataFall(stageSquid.isIs_persisted(),
                stageSquid.getColumns(), stageSquid.getDestination_squid_id(),
                stageSquid.getId(), stageSquid.getName(),
                stageSquid.isTruncate_existing_data_flag(),
                stageSquid.getTable_name(), stageSquid.getCdc());
    }

    /**
     * 落地squid.
     *
     * @param stageSquid
     * @return
     */
    public TSquid doTranslateDataFall(DataSquid stageSquid) {
        return doTranslateDataFall(stageSquid.isIs_persisted(),
                stageSquid.getColumns(), stageSquid.getDestination_squid_id(),
                stageSquid.getId(), stageSquid.getName(),
                stageSquid.isTruncate_existing_data_flag(),
                stageSquid.getTable_name(), stageSquid.getCdc());

        /**
         if (stageSquid.isIs_persisted()) { // 设置为落地
         // 默认表结构已经生成了, 表名为： sf_implied.squidFlowId + _ + squidId
         // 1. 获取所有的columns，
         // 2. 根据column信息，翻译成 TColumn
         // 3. TDataSource信息
         Set<TColumn> tColumnMap = new HashSet<>();
         List<Column> columns = stageSquid.getColumns();
         for (Column c : columns) {
         TColumn tColumn = new TColumn();
         tColumn.setId(c.getId());
         tColumn.setName(c.getName());
         tColumn.setLength(c.getLength());
         tColumn.setPrecision(c.getPrecision());
         tColumn.setPrimaryKey(c.isIsPK());
         tColumn.setData_type(TDataType.sysType2TDataType(c.getData_type()));
         tColumn.setNullable(c.isNullable());
         tColumn.setCdc(c.getCdc() == 1 ? 2 : 1);		//  后台的是=2，否=1
         tColumn.setBusinessKey(c.getIs_Business_Key() == 1 ? true : false);
         tColumnMap.add(tColumn);
         }

         if (stageSquid.getDestination_squid_id() == 0) { // 设置落地但没有指定落地库。隐式落地。

         TDataSource tDataSource = new TDataSource();

         tDataSource.setType(DataBaseType.HBASE_PHOENIX);
         // 隐式落地,改用设置的表名
         //tDataSource.setTableName(HbaseUtil.genImpliedTableName(this.ctx.getRepositoryId(), stageSquid.getSquidflow_id(), stageSquid.getId()));
         tDataSource.setTableName(stageSquid.getTable_name());
         tDataSource.setHost(ConfigurationUtil.getInnerHbaseHost() + ":" + ConfigurationUtil.getInnerHbasePort());
         //				tDataSource.setPort(ConfigurationUtil.getInnerHbasePort());
         tDataSource.setCDC(stageSquid.getCdc()==1?true:false);
         // TDataSource tDataSource = new TDataSource("192.168.137.2",
         // 3306,
         // "squidflowtest", "root", "root", stageSquid.getSquidflow_id()
         // +
         // "_" + stageSquid.getId(), DataBaseType.MYSQL);

         TDataFallSquid tDataFallSquid = new TDataFallSquid();
         tDataFallSquid.setTruncateExistingData(stageSquid.isTruncate_existing_data_flag()==0?false:true);
         tDataFallSquid.setPreviousSquid(this.getCurLastTSqud());
         tDataFallSquid.setDataSource(tDataSource);
         tDataFallSquid.setSquidId(stageSquid.getId());
         tDataFallSquid.setType(TSquidType.DATA_FALL_SQUID);
         tDataFallSquid.setColumnSet(tColumnMap);
         tDataFallSquid.setName(stageSquid.getName());
         return tDataFallSquid;

         } else { // 指定地点的落地。

         int squidId = stageSquid.getDestination_squid_id();
         Squid destSquid = ctx.getSquidById(squidId);
         if (SquidTypeEnum.parse(destSquid.getSquid_type()) == SquidTypeEnum.DBSOURCE) {
         DbSquid dbSquid = (DbSquid) destSquid;

         TDataFallSquid tdf = new TDataFallSquid();
         tdf.setTruncateExistingData(stageSquid.isTruncate_existing_data_flag()==0?false:true);
         TDataSource tDataSource = new TDataSource();
         tDataSource.setCDC(stageSquid.getCdc() == 1);
         tDataSource.setDbName(dbSquid.getDb_name());
         tDataSource.setUserName(dbSquid.getUser_name());
         tDataSource.setPassword(dbSquid.getPassword());
         tDataSource.setType(DataBaseType.parse(dbSquid.getDb_type()));
         tDataSource.setTableName(stageSquid.getTable_name());
         tDataSource.setHost(dbSquid.getHost());
         tDataSource.setPort(dbSquid.getPort());

         tdf.setPreviousSquid(this.getCurLastTSqud());
         tdf.setDataSource(tDataSource);
         tdf.setSquidId(stageSquid.getId());
         tdf.setType(TSquidType.DATA_FALL_SQUID);
         tdf.setName(stageSquid.getName());
         tdf.setColumnSet(tColumnMap);
         return tdf;
         } else if (SquidTypeEnum.parse(destSquid.getSquid_type()) == SquidTypeEnum.MONGODB) {
         NOSQLConnectionSquid nosqlConnectionSquid = (NOSQLConnectionSquid)destSquid;
         NoSQLDataBaseType dataBaseType = NoSQLDataBaseType.parse(nosqlConnectionSquid.getDb_type());
         switch (dataBaseType) {
         case MONGODB:
         TMongoDataFallSquid tdf = new TMongoDataFallSquid();
         tdf.setTruncateExistingData(stageSquid.isTruncate_existing_data_flag()==0?false:true);

         TNoSQLDataSource tDataSource = new TNoSQLDataSource();
         tDataSource.setCDC(stageSquid.getCdc() == 1);
         tDataSource.setDbName(nosqlConnectionSquid.getDb_name());
         tDataSource.setUserName(nosqlConnectionSquid.getUser_name());
         tDataSource.setPassword(nosqlConnectionSquid.getPassword());
         tDataSource.setType(dataBaseType);
         tDataSource.setTableName(stageSquid.getTable_name());
         tDataSource.setHost(nosqlConnectionSquid.getHost());
         tDataSource.setPort(nosqlConnectionSquid.getPort());

         tdf.setPreviousSquid(this.getCurLastTSqud());
         tdf.setDataSource(tDataSource);
         tdf.setSquidId(stageSquid.getId());
         tdf.setName(stageSquid.getName());
         tdf.setColumnSet(tColumnMap);
         return tdf;
         default:
         throw new RuntimeException("不能处理该NOSQL数据库类型" + dataBaseType);
         }
         }
         }
         }
         return null;
         */
    }

    /**
     * 取JdbcTemplate.
     *
     * @return
     */
    public JdbcTemplate getCurJdbc() {
        return ConstantUtil.getJdbcTemplate();
    }

    /**
     * 翻译squid filter.
     *
     * @param sd
     * @return
     */
    public TSquid doTranslateSquidFilter(Squid sd) {
        String filterString = sd.getFilter().trim();

        if (!ValidateUtils.isEmpty(filterString)) {
            TFilterExpression tfe = TranslateUtil.translateTFilterExpression(sd, ctx.getVariable(), ctx.name2squidMap);
            if (tfe != null) {
                TFilterSquid tfs = new TFilterSquid();
                tfs.setSquidId(sd.getId());

                // 设置previous
                TSquid prevTSquid = this.getCurLastTSqud();
                if (prevTSquid == null) {
                    List<Squid> prevs = this.ctx.getPrevSquids(sd);
                    if (prevs.size() == 0) throw new RuntimeException("未找到本squid的前置squid.");
                    Squid prevSquid = prevs.get(0);
                    prevTSquid = this.ctx.getSquidOut(prevSquid);
                }
                tfs.setPreviousSquid(prevTSquid);
                tfs.setFilterExpression(tfe);
                tfs.setName(sd.getName());
                return tfs;
            }
        }
        return null;
    }

    /**
     * 翻译squid filter, 采用dataframe的filter API
     *
     * @param sd
     * @return
     */
    public TSquid doTranslateNewSquidFilter(DataSquid sd) {
        String filterString = sd.getFilter().trim();
        Squid prevSquid = null;
        // 暂时 filter 不允许出现变量
        if (!ValidateUtils.isEmpty(filterString)) {
            TFilterSquid tfs = new TFilterSquid();
            tfs.setSquidId(sd.getId());
            // 设置 previous
            TSquid preTSquid = this.getCurLastTSqud();
            // stage ->  join -> filter
            // exception => exception -> filter
            if (preTSquid == null) {     // 代表当前squid没有join
                List<Squid> preSquids = this.ctx.getPrevSquids(sd);
                if (preSquids.size() == 0) {
                    throw new RuntimeException("未找到本squid的前置squid.");
                }
                prevSquid = preSquids.get(0);
                preTSquid = this.ctx.getSquidOut(prevSquid);
                // id2columns 为了和 rdd兼容
                // 1 获取上游 squid
                // 提取上游squid的column 与id映射关系
                Map<Integer, TStructField> id2column = TranslateUtil.getId2ColumnsFromSquid(prevSquid);
                List<Map<Integer, TStructField>> id2Columns = new ArrayList<>();
                id2Columns.add(id2column);
                tfs.setId2Columns(id2Columns);
                // 设置squidName, 没有join的时候,注册的是上游squid的名字,有join的不需要再注册
                tfs.setSquidName(prevSquid.getName());
            } else {    // 存在join,union
                if (sd instanceof StageSquid) {
                    StageSquid stageSquid = (StageSquid) sd;
                    List<SquidJoin> squidJoins = stageSquid.getJoins();
                    // 按照 prior_join_id 增序
                    Collections.sort(squidJoins, new Comparator<SquidJoin>() {
                        @Override
                        public int compare(SquidJoin o1, SquidJoin o2) {
                            return o1.getPrior_join_id() - o2.getPrior_join_id();
                        }
                    });
                    if (squidJoins.get(1).getJoinType() > JoinType.BaseTable.value()
                            && squidJoins.get(1).getJoinType() < JoinType.Unoin.value()) {
                        // join
                        tfs.setId2Columns(
                                TranslateUtil.getId2ColumnsFormStageSquid(squidJoins, ctx));
                    } else {
                        // union
                        return this.doTranslateSquidFilter(sd);
                    }
                } else if (sd instanceof ExceptionSquid) {
//                    tfs.setId2Columns(Arrays.asList(TranslateUtil.getId2ColumnsFromSquid(ctx.getPrevSquids(sd).get(0))));
                    return this.doTranslateSquidFilter(sd);
                }
//                tfs.setId2Columns();
            }
            tfs.setPreviousSquid(preTSquid);
            TFilterExpression tfe = new TFilterExpression();
            tfe.setSourceExpression(filterString);
            tfs.setFilterExpression(tfe);

            return tfs;
        }
        return null;
    }


    /**
     * 翻译 extractSquid的transformation
     *
     * @param ds
     * @return
     */
    protected TSquid doExtractTranslateTransformation(DataSquid ds) {
        return doExtractTranslateTransformation(ds, null);
    }

    /**
     * 翻译 extractSquid的transformation
     * 可以在tranformation 的 InfoMap中添加一些自定义信息
     *
     * @param ds
     * @param customInfo
     * @return
     */
    protected TSquid doExtractTranslateTransformation(DataSquid ds, Map<String, Object> customInfo) {
        TTransformationSquid ts = new TTransformationSquid();
        List<TTransformationAction> actions = new ArrayList<>();
        ts.settTransformationActions(actions);
        for (TransformationLink link : ds.getTransformationLinks()) {
            Transformation from_trs = this.getTrsById(link.getFrom_transformation_id());
            Transformation to_trs = this.getTrsById(link.getTo_transformation_id());
            TTransformationAction action = new TTransformationAction();
            TTransformation tTransformation = new TTransformation();
            tTransformation.setType(TransformationTypeEnum.VIRTUAL);
            tTransformation.setSequenceIndex(1);
            tTransformation.setInKeyList(new JList<>(-from_trs.getColumn_id()));
            tTransformation.setOutKeyList(new JList<>(to_trs.getColumn_id()));
            HashSet<Integer> dtk = getDropedTrsKeys(to_trs);
            dtk.add(-from_trs.getColumn_id());
            action.setRmKeys(dtk);


            Column col = this.getColumnById(ds, to_trs.getColumn_id());
            HashMap<String, Object> infoMap = new HashMap<>();
            infoMap.put(TTransformationInfoType.STRING_MAX_LENGTH.dbValue, col.getLength());
            infoMap.put(TTransformationInfoType.NUMERIC_PRECISION.dbValue, col.getPrecision());
            infoMap.put(TTransformationInfoType.NUMERIC_SCALE.dbValue, col.getScale());
            infoMap.put(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue, col.getData_type());
            infoMap.put(TTransformationInfoType.VIRT_TRANS_OUT_DATA_TYPE.dbValue, col.getData_type());
            infoMap.put(TTransformationInfoType.IS_NULLABLE.dbValue, col.isNullable());

            // 添加自定义的info
            if (customInfo != null) {
                infoMap.putAll(customInfo);
            }

            tTransformation.setInfoMap(infoMap);


            action.settTransformation(tTransformation);
            actions.add(action);
        }
        // 添加 extraction_date
        Column dateCol = getExtractionDateColumn();
        if (dateCol != null) {
            TTransformationAction action = new TTransformationAction();
            TTransformation tTransformation = new TTransformation();
            tTransformation.setType(TransformationTypeEnum.CONSTANT);
            tTransformation.setSequenceIndex(1);
            tTransformation.setOutKeyList(new JList<Integer>(dateCol.getId()));
            action.settTransformation(tTransformation);
            JMap infoMap = new JMap<>();
            infoMap.put(TTransformationInfoType.CONSTANT_VALUE.dbValue, DateUtil.nowTimestamp());
            infoMap.put(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue, TDataType.sysType2TDataType(
                    SystemDatatype.DATETIME.value()));
            // 添加自定义的info
            if (customInfo != null) {
                infoMap.putAll(customInfo);
            }
            tTransformation.setInfoMap(infoMap);
            actions.add(action);
        }
        ts.setPreviousSquid(this.getCurLastTSqud());
        return ts;
    }

    protected Map<Integer, Transformation> getId2Trans() {
        if (id2trans == null) {
            id2trans = new HashMap<>();
            DataSquid ss = (DataSquid) this.currentSquid;
            for (Transformation tf : ss.getTransformations()) {
                id2trans.put(tf.getId(), tf);
            }
        }
        return id2trans;
    }

    protected Transformation getTrsById(int trsId) {
        return getId2Trans().get(trsId);
    }

    /**
     * 获取系统列extractionDate
     *
     * @return 存在返回, 不存在返回null
     */
    protected Column getExtractionDateColumn() {
        DataSquid de = (DataSquid) this.currentSquid;
        for (Column col : de.getColumns()) {
            if (col.getName().equals(ConstantsUtil.CN_EXTRACTION_DATE)) {   // 名字匹配
                // 判断是否是系统列,需要判断是否有link连接上
                List<TransformationLink> links = de.getTransformationLinks();
                List<Transformation> trans = de.getTransformations();
                for (Transformation tran : trans) {
                    if (tran.getColumn_id() == col.getId()) { // 匹配column上的tran
                        Transformation t = null;
                        for (TransformationLink link : links) {
                            if (link.getTo_transformation_id() == tran.getId()) {
                                t = tran;
                                break;
                            }
                        }
                        if (t == null) { // 没有link连接该tran
                            return col;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * 抛弃transformation.并且返回可用的抛弃值。
     *
     * @param tf
     * @return
     */
    protected HashSet<Integer> getDropedTrsKeys(Transformation tf) {
        HashSet<Integer> ret = new HashSet<>();
        if (!this.removeNoUseKeys) {
            DataSquid ss = (DataSquid) this.currentSquid;
            for (ReferenceColumn col : ss.getSourceColumns()) {
                boolean contains = false;
                for (Transformation t : this.getBeginTrs()) {
                    if (t.getColumn_id() == col.getColumn_id()) {
                        contains = true;
                    }
                }
                if (!contains) {
                    ret.add(col.getColumn_id());
                }
            }
            this.removeNoUseKeys = true;

        }
        return ret;
    }

    /**
     * 取开头的squid.
     *
     * @return
     */
    protected List<Transformation> getBeginTrs() {
        if (beginTrs == null) {
            beginTrs = new ArrayList<>();
            DataSquid ss = (DataSquid) this.currentSquid;
            // 列多的情况下,性能太差, 转数据库查询  TODO


            for (ReferenceColumn col : ss.getSourceColumns()) {
                for (TransformationLink link : ss.getTransformationLinks()) {
                    Transformation tf = this.getTrsById(link.getFrom_transformation_id());
                    if (tf.getColumn_id() == col.getColumn_id()) {
                        beginTrs.add(tf);
                        break;
                    }
                }
            }
        }

        return beginTrs;
    }
}
