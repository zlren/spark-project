package test;

import lab.zlren.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * JDBC辅助组件测试类
 * Created by zlren on 2017/4/26.
 */
public class JDBCHelperTest {
    public static void main(String[] args) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // jdbcHelper.executeUpdate(
        //         "insert into test_user(name,age) values(?,?)",
        //         new Object[]{"王二", 28});

        // final Map<String, Object> testUser = new HashMap<>();
        // jdbcHelper.executeQuery(
        //         "select name,age from test_user where age=?",
        //         new Object[]{28},
        //         new JDBCHelper.QueryCallback() {
        //
        //             @Override
        //             public void process(ResultSet rs) throws Exception {
        //                 if (rs.next()) {
        //                     String name = rs.getString(1);
        //                     int age = rs.getInt(2);
        //
        //                     // 匿名内部类的使用，有一个很重要的知识点
        //                     // 如果要访问外部类中的一些成员，比如方法内的局部变量
        //                     // 那么，必须将局部变量，声明为final类型，才可以访问
        //                     // 否则是访问不了的
        //                     testUser.put("name", name);
        //                     testUser.put("age", age);
        //                 }
        //             }
        //
        //         });
        // System.out.println(testUser.get("name") + ":" + testUser.get("age"));


        // 测试批量执行SQL语句
        String sql = "insert into test_user(name, age) values(?, ?)";

        List<Object[]> paramsList = new ArrayList<>();
        paramsList.add(new Object[]{"麻子", 30});
        paramsList.add(new Object[]{"王五", 35});

        jdbcHelper.executeBatch(sql, paramsList);
    }
}
