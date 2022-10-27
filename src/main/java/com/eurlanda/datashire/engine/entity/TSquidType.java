package com.eurlanda.datashire.engine.entity;

public enum TSquidType {

  DEBUG_SQUID(0),
  TRANSFORMATION_SQUID(1),
  JOIN_SQUID(2),
  DATABASE_SQUID(3),
  AGGREGATE_SQUID(4),
  TRAIN_SQUID(5),
  DATA_FALL_SQUID(6),
  REPORT_SQUID(7),
  UNION_SQUID(8),
  FILTER_SQUID(9),
  INNEREXTRACT_SQUID(10),
  FTP_FILE_SQUID(11),
  EXCEPTION_SQUID(12),
  SHARED_FILE_SQUID(13),
  HDFS_SQUID(14),
  // 去重
  DISTINCT_SQUID(15),
  QUANTIFY_TRAIN_SQUID(16),
  DISCRETIZE_TRAIN_SQUID(17),
  ALS_TRAIN_SQUID(18),
  KMEANS_TRAIN_SQUID(19),
  LINEARREGRESSION_TRAIN_SQUID(20),
  LOGISTICREGRESSION_TRAIN_SQUID(21),
  NAIVEBAYS_TRAIN_SQUID(22),
  RIDGEREGRESSION_TRAIN_SQUID(23),
  SVM_TRAIN_SQUID(24),
  DECISIONTREE_TRAIN_SQUID(25),
  MONGO_DATA_FALL_SQUID(26),
  MONGO_EXTRACT_SQUID(27),

  //dest
  DEST_ES_SQUID(28),
  HBASE_SQUID(30),
  DEST_HDFS_SQUID(31),

  ASSOCIATIONRULES_SQUID(32),

  DEST_IMPALA_SQUID(33),
  GROUP_TAGGING_SQUID(34),

  HIVE_EXTRACT_SQUID(35),
  CASSANDRA_EXTRACT_SQUID(36),
  USER_DEFINED_SQUID(37),
  DEST_SYSTEM_HIVE_SQUID(38),
  STATISTICS_SQUID(39),
  DEST_CASSANDRA_SQUID(38),

  RANDOMFOREST_CLASSIFICATION_SQUID(41),
  LASSOREGRESSION_WITH_ELASTICNET_SQUID(42),
  RANDOMFOREST_REGRESSION_SQUID(43),
  MULTILAYER_PERCEPTRON_CLASSIFICATION_SQUID(44),
  NORMALIZER_SQUID(45),
  PARTIALLEASTSQUARES_SQUID(46),
  DATACATCH_SQUID(47),
  COEFFICIENT_SQUID(48),
  DECISION_TREE_REGRESSION_SQUID(49),
  DECISION_TREE_CLASSIFICATIONS_QUID(50),
  LOGISTIC_REGRESSION_SQUID(51),
  LINEAR_REGRESSION_SQUID(52),
  RIDGE_REGRESSION_SQUID(53),
  BISECTING_K_MEANS_SQUID(54),
  SAMPLINGSQUID(55),
  PIVOTSQUID(56),
    ;

  private int value;

  private TSquidType(int v) {
    value = v;
  }

  public int value() {
    return value;
  }

  public static TSquidType parse(int t) {
    for (TSquidType result : values()) {
      if (result.value() == t) {
        return result;
      }
    }
    return null;
  }

  public static TSquidType parse(String name) {
    return (TSquidType) Enum.valueOf(TSquidType.class, name);
  }

}
