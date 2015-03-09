/**
 *
 */
package com.spankr.tutorial.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lee_vettleson
 *
 */
public final class DataHelper {

    private final static Logger log = LoggerFactory.getLogger(DataHelper.class);

    /**
     * Let's load our data source!
     *
     * @return
     */
    public static DataSource getDatasource(final String propertiesFile) {
        log.info("Creating datasource");
        InputStream is;
        Properties properties = new Properties();
        try {
            is = TestLoadPreferenceData.class.getResourceAsStream(propertiesFile);
            properties.load(is);
            is.close();
        } catch (IOException e) {
            log.info("Unable to load " + propertiesFile);
        }

        log.debug("Datasource configuration - ");
        String key;
        Iterator<Object> pit = properties.keySet().iterator();
        while (pit.hasNext()) {
            key = (String) pit.next();
            if (key.contains("pass")) {
                continue;
            }
            log.debug(String.format("   %-12s: %s", key, properties.getProperty(key)));
        }
        BasicDataSource ds = new BasicDataSource();
        ds.setUsername(properties.getProperty("jdbc.username"));
        ds.setPassword(properties.getProperty("jdbc.password"));
        ds.setDriverClassName(properties.getProperty("jdbc.driver"));
        ds.setUrl(properties.getProperty("jdbc.url"));

        return ds;
    }

}
