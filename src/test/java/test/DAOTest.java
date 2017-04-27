package test;

import lab.zlren.sparkproject.dao.DAOFactory;
import lab.zlren.sparkproject.dao.TaskDAO;
import lab.zlren.sparkproject.domain.Task;

/**
 * dao测试类
 * Created by zlren on 2017/4/26.
 */
public class DAOTest {
    public static void main(String[] args) {
        TaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(task.getTaskName());
    }
}
