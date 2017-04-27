package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC增删改查示范类
 * Created by zlren on 2017/4/26.
 */
public class JdbcCRUD {
    public static void main(String[] args) {
        insert();
    }

    private static void insert() {
        Connection connection = null;
        Statement statement = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://10.109.247.143:3306/spark", "zlren", "Lab2016!");
            statement = connection.createStatement();
            int n = statement.executeUpdate("INSERT INTO student_infos(name, age) VALUES('zlren', 24) ");
            System.out.println("sql 语句影响的行数: " + n);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
