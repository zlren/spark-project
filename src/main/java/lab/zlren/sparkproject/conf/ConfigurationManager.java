package lab.zlren.sparkproject.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 从properties文件中读取配置项，提供外接读取某个配置key对应的方法
 * Created by zlren on 2017/4/26.
 */
public class ConfigurationManager {

    private static Properties properties = new Properties();

    /**
     * 在静态代码块中读取配置文件
     */
    static {
        try {
            InputStream inputStream = ConfigurationManager.class.getClassLoader().getResourceAsStream(
                    "conf.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static Integer getInteger(String key) {
        return Integer.valueOf(properties.getProperty(key));
    }

    public static Boolean getBoolean(String key) {
        return Boolean.valueOf(properties.getProperty(key));
    }
}
