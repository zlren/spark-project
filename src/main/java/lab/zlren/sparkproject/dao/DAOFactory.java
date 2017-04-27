package lab.zlren.sparkproject.dao;

/**
 * dao工厂
 * Created by zlren on 2017/4/26.
 */
public class DAOFactory {
    public static TaskDAO getTaskDAO() {
        return new TaskDAO();
    }
}
