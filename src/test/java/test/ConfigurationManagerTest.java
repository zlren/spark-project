package test;

import lab.zlren.sparkproject.conf.ConfigurationManager;

/**
 * Created by zlren on 2017/4/26.
 */
public class ConfigurationManagerTest {
    public static void main(String[] args) {
        String v1 = ConfigurationManager.getProperty("key1");
        String v2 = ConfigurationManager.getProperty("key2");
        System.out.println(v1);
        System.out.println(v2);
    }
}
