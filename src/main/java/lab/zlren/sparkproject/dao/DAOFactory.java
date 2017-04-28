package lab.zlren.sparkproject.dao;

/**
 * dao工厂，工厂设计模式
 * Created by zlren on 2017/4/26.
 */
public class DAOFactory {
    public static TaskDAO getTaskDAO() {
        return new TaskDAO();
    }

    public static SessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAO();
    }
}
