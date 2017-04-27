package lab.zlren.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import lab.zlren.sparkproject.conf.ConfigurationManager;
import lab.zlren.sparkproject.constant.Constants;
import lab.zlren.sparkproject.dao.DAOFactory;
import lab.zlren.sparkproject.dao.TaskDAO;
import lab.zlren.sparkproject.data.MockData;
import lab.zlren.sparkproject.domain.Task;
import lab.zlren.sparkproject.util.MyStringUtils;
import lab.zlren.sparkproject.util.ParamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;

/**
 * 用户访问session分析作业
 * <p>
 * 过滤条件
 * 1 时间范围
 * 2 性别
 * 3 年龄
 * 4 职业
 * 5 城市
 * 6 搜索词
 * 7 点击品类
 * <p>
 * <p>
 * Created by zlren on 2017/4/26.
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc.sc());

        // 生成模拟数据（user_visit_action、user_info）
        mockData(sc, sqlContext);

        TaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 进行session粒度的聚合，从user_visit_action表中查询出来指定日期范围内的行为数据
        // 提交任务到task表，相关参数会以json格式写在表中，然后把json串作为参数去调用spark-submit的shell脚本
        // 参数会传递到main函数的args参数中，所以从args中取出相关（筛选）参数
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);

        // 参数
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 根据时间过滤
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
        // 聚合，从session行为表和user表中共同组装出了想要的数据
        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(actionRDD, sqlContext);

        // 接下来是对指定筛选条件进行过滤


        sc.close();
    }

    /**
     * 获取sql context
     * 本地运行：sqlcontext
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据
     *
     * @param javaSparkContext
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext javaSparkContext, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(javaSparkContext, sqlContext);
        }
    }

    /**
     * 获取指定范围内的用户访问行为数据
     *
     * @param sqlContext
     * @param taskParam
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {

        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'";
//				+ "and session_id not in('','','')"

        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param actionRDD 行为数据rdd
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext sqlContext) {

        // row -> (session_id, row)
        // 把一条row中的session_id取出来当做tuple的第一个值，row本身当做第二个值
        JavaPairRDD<String, Row> pariedActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                //　session_id的位置是2，可以在mock生成模拟数据注册表那里去看
                return new Tuple2<>(row.getString(2), row);
            }
        });

        // session_id, Iterator
        // iterator可以迭代一个session_id对应的多个行为
        JavaPairRDD<String, Iterable<Row>> groupedActionRDD = pariedActionRDD.groupByKey();

        // grouped后，第二个参数是所有对应一个session_id的所有row，从中抽取出search_word和click_category_id并按格式拼装字符串
        // 也就是只把这里感兴趣的信息抽取出来作为返回tuple的第二个值
        // 并且以user_id作为tuple的第一个值，方便和user_info表进行join操作
        // (userid, partAggrInfo(包含sessionid、searchKeywords和clickCategoryIds))
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = groupedActionRDD.mapToPair(new PairFunction<Tuple2<String,
                Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {

                String sessionid = tuple2._1;
                Iterator<Row> iterator = tuple2._2.iterator();

                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                Long userid = null;

                while (iterator.hasNext()) {
                    Row row = iterator.next();

                    // 一个sessionid对应的多条行为数据，它们的userid一样吗？当然一样!!
                    if (userid == null) {
                        userid = row.getLong(1);
                    }

                    String searchKeyword = row.getString(5);
                    Long clickCategoryId = row.getLong(6);

                    if (MyStringUtils.isNotEmpty(searchKeyword)) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                            searchKeywordsBuffer.append(searchKeyword + ",");
                        }
                    }
                    if (clickCategoryId != null) {
                        if (!clickCategoryIdsBuffer.toString().contains(
                                String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(clickCategoryId + ",");
                        }
                    }


                }

                // 去掉前后的逗号
                String searchKeywords = MyStringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = MyStringUtils.trimComma(clickCategoryIdsBuffer.toString());

                // key=value|key=value
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" +
                        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
                        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

                return new Tuple2<>(userid, partAggrInfo);
            }
        });

        // 查询user_info表
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        // (userid, row)
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });


        // join操作
        // (userid, (aggrpartinfo, userinforow))
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        // 把上面的(aggrpartinfo, userinforow)这个tuple摘出有效信息组成string
        // userid, fullinfo
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {

                String partAggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String sessionid = MyStringUtils.getFieldFromConcatString(partAggrInfo, "\\|",
                        Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<>(sessionid, fullAggrInfo);
            }
        });


        return sessionid2FullAggrInfoRDD;
    }


    /**
     * 过滤session数据
     *
     * @param session2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> session2AggrInfoRDD,
                                                             final JSONObject taskParam) {

        JavaPairRDD<String, String> filteredSession2AggrInfoRDD = session2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>
                () {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                // 过滤逻辑
                String aggrInfo = tuple._2;

                // 年龄范围 startAge endAge
                Integer age = Integer.valueOf(MyStringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_AGE));



                return null;
            }
        });


        return null;
    }
}
