/**
 *
 */
package com.spankr.tutorial.data;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseDataSourceConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.util.fileloader.DataFileLoader;
import org.dbunit.util.fileloader.FlatXmlDataFileLoader;
import org.dbunit.util.fileloader.FullXmlDataFileLoader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lee_vettleson
 *
 */
public class TestLoadArticleData {

    private final static Logger log = LoggerFactory.getLogger(TestLoadArticleData.class);
    public final static String LIVE_DB = "/liveDB.properties";

    @BeforeClass
    public static void setupTests() throws SQLException, IOException {
        DataSource dsrc = DataHelper.getDatasource(LIVE_DB);
        assertNotNull("No datasource", dsrc);

        Connection con = dsrc.getConnection();
        assertNotNull("No connection", con);

        InputStream is = TestLoadPreferenceData.class.getResourceAsStream("/schemas/liveDB-schema.sql");
        assertNotNull(is);
        Statement stmt = con.createStatement();
        assertNotNull("No statement", stmt);
        stmt.execute(IOUtils.toString(is, "UTF-8"));

        con.close();
        IOUtils.closeQuietly(is);
    }

    /**
     * Lets load us up some authors!
     *
     * @throws SQLException
     */
    @Test
    public void loadAuthorData() throws SQLException {
        DataSource dsrc = DataHelper.getDatasource(LIVE_DB);

        IDatabaseConnection dbUnitCon = new DatabaseDataSourceConnection(dsrc);

        Map<String, String> substitutes = new HashMap<String, String>();
        substitutes.put("[null]", null);

        DataFileLoader loader = new FullXmlDataFileLoader(substitutes);

        IDataSet ds = loader.load("/data/articles/authors.xml");
        assertNotNull("No dataset found", ds);

        try {
            DatabaseOperation.REFRESH.execute(dbUnitCon, ds); // refresh of the DB.

        } catch (DatabaseUnitException e) {
            log.error("Failed to load the data: {}", e);
            fail(e.getMessage());
        } finally {
            dbUnitCon.close();
        }

        Connection con = dsrc.getConnection();
        try {
            Statement stmt = con.createStatement();
            ResultSet results = stmt.executeQuery("SELECT FIRST_NAME, LAST_NAME, LANG_CODE FROM ARTICLE_AUTHOR WHERE AUTHOR_ID=9001 AND LANG_CODE='en-US'");
            assertTrue("ResultSet is empty", results.next());
            assertEquals("Frednando", results.getString("FIRST_NAME"));
            assertEquals("Trinsomething", results.getString("LAST_NAME"));
        } finally {
            con.close();
        }
    }

    /**
     * Lets load us up some articles!
     *
     * @throws SQLException
     */
    @Test
    public void loadArticleData() throws SQLException {
        IDatabaseConnection dbUnitCon = new DatabaseDataSourceConnection(DataHelper.getDatasource(LIVE_DB));

        Map<String, String> substitutes = new HashMap<String, String>();
        substitutes.put("[null]", null);

        DataFileLoader loader = new FullXmlDataFileLoader(substitutes);

        IDataSet ds = loader.load("/data/articles/articles.xml");
        assertNotNull("No dataset found", ds);

        try {
            DatabaseOperation.REFRESH.execute(dbUnitCon, ds); // refresh of the DB.

        } catch (DatabaseUnitException e) {
            log.error("Failed to load the data: {}", e);
            fail(e.getMessage());
        } finally {
            dbUnitCon.close();
        }
    }

    @Test
    public void loadArticleGlue() throws SQLException {
        IDatabaseConnection dbUnitCon = new DatabaseDataSourceConnection(DataHelper.getDatasource(LIVE_DB));

        Map<String, String> substitutes = new HashMap<String, String>();
        substitutes.put("[null]", null);

        DataFileLoader loader = new FlatXmlDataFileLoader(substitutes);

        IDataSet ds = loader.load("/data/articles/article-glue.xml");
        assertNotNull("No dataset found", ds);

        try {
            DatabaseOperation.REFRESH.execute(dbUnitCon, ds); // refresh of the DB.
        } catch (DatabaseUnitException e) {
            log.error("Failed to load the data: {}", e);
            fail(e.getMessage());
        } finally {
            dbUnitCon.close();
        }
    }

}
