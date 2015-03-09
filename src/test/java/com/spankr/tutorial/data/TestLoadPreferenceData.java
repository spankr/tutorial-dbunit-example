/**
 *
 */
package com.spankr.tutorial.data;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseDataSourceConnection;
import org.dbunit.database.DatabaseSequenceFilter;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.util.fileloader.DataFileLoader;
import org.dbunit.util.fileloader.FlatXmlDataFileLoader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A couple test cases to validate a good db connection and to upload marketing
 * (preferences) data to the db.
 *
 * @author Lee_Vettleson
 *
 */
public class TestLoadPreferenceData {

    private final static Logger log = LoggerFactory.getLogger(TestLoadPreferenceData.class);
    public final static String PREF_DB = "/prefDB.properties";

    @BeforeClass
    public static void setupTests() throws SQLException, IOException {
        DataSource dsrc = DataHelper.getDatasource(PREF_DB);
        assertNotNull("No datasource", dsrc);

        Connection con = dsrc.getConnection();
        assertNotNull("No connection", con);

        InputStream is = TestLoadPreferenceData.class.getResourceAsStream("/schemas/preferencesDB-schema.sql");
        assertNotNull(is);
        Statement stmt = con.createStatement();
        assertNotNull("No statement", stmt);
        stmt.execute(IOUtils.toString(is, "UTF-8"));

        con.close();
        IOUtils.closeQuietly(is);
    }

    /**
     * Check our db connection to make sure its configured properly
     *
     * @throws SQLException
     */
    @Test
    public void tryToConnect() throws SQLException {
        DataSource ds = DataHelper.getDatasource(PREF_DB);
        assertNotNull("No datasource", ds);

        Connection con = ds.getConnection();
        assertNotNull("No connection", con);

        Statement stmt = con.createStatement();
        assertNotNull("No statement", stmt);

        String sql = "select count(1) from INFORMATION_SCHEMA.SYSTEM_TABLES";
        ResultSet rs = stmt.executeQuery(sql);
        assertTrue("ResultSet is empty", rs.next());
        log.info("There are {} records in INFORMATION_SCHEMA.SYSTEM_TABLES", rs.getInt(1));
    }

    /**
     * Lets load us up some test data!
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void loadTestData() throws SQLException, IOException {
        DataSource dsrc = DataHelper.getDatasource(PREF_DB);
        IDatabaseConnection dbUnitCon = new DatabaseDataSourceConnection(dsrc);

        // dbUnit can't see missing attributes as null values (in the first table entry) so we'll
        // substitute '[null]' for null values
        Map<String, String> substitutes = new HashMap<String, String>();
        substitutes.put("[null]", null);

        DataFileLoader loader = new FlatXmlDataFileLoader(substitutes);

        IDataSet ds = loader.load("/data/marketing-preferences.xml"); // pulls this from classpath, yay!
        assertNotNull("No dataset found", ds);

        try {
            DatabaseOperation.CLEAN_INSERT.execute(dbUnitCon, ds); // clean load of the DB. Careful, clean means "delete the old stuff"
        } catch (DatabaseUnitException e) {
            log.error("Failed to load the data: {}", e);
            fail(e.getMessage());
        } finally {
            dbUnitCon.close();
        }
    }

    /**
     * Lets look for some data and dump it out to the logs
     *
     * @throws SQLException
     * @throws DataSetException
     */
    @Test
    public void checkForData() throws SQLException, DataSetException {
        IDatabaseConnection dbunitConnection = new DatabaseDataSourceConnection(DataHelper.getDatasource(PREF_DB));

        // Let's filter to keep only the tables we want
        String[] tables = {"CATEGORY", "ALERT_TYPE"};
        ITableFilter filter = new DatabaseSequenceFilter(dbunitConnection, tables);
        IDataSet dataset = new FilteredDataSet(filter, dbunitConnection.createDataSet());

        ITableIterator tabIT = dataset.iterator();
        ITable t;
        while (tabIT.next()) {
            t = tabIT.getTable();
            Column[] cols = tabIT.getTableMetaData().getColumns();
            log.info("Table name: {}", t.getTableMetaData().getTableName());
            for (int i = 0; i < t.getRowCount(); i++) {
                log.info("Row {}", i);
                for (Column c : cols) {
                    log.info("\t{} = {}", c.getColumnName(), t.getValue(i, c.getColumnName()));
                }
            }
        }
    }

    /**
     * Lets try an assertion on the database (database validation, baby!)
     *
     * @throws SQLException
     * @throws DatabaseUnitException
     */
    @Test
    public void performDataAssert() throws SQLException, DatabaseUnitException {
        IDatabaseConnection dbUnitCon = new DatabaseDataSourceConnection(DataHelper.getDatasource(PREF_DB));

        // dbUnit can't see missing attributes as null values (in the first table entry) so we'll
        // substitute '[null]' for null values
        Map<String, String> substitutes = new HashMap<String, String>();
        substitutes.put("[null]", null);

        DataFileLoader loader = new FlatXmlDataFileLoader(substitutes);

        IDataSet ds = loader.load("/data/marketing-preferences.xml"); // pulls this from classpath,
        DatabaseOperation.CLEAN_INSERT.execute(dbUnitCon, ds); // clean load of the DB. Careful,

        final IDataSet EXPECTED = loader.load("/data/assert-this.xml"); // pulls this from classpath, yay!
        assertNotNull("No dataset found", EXPECTED);

        // Now let's check our database
        String[] tables = {"ALERT_TYPE", "USER_ANSWER"};
        ITableFilter filter = new DatabaseSequenceFilter(dbUnitCon, tables);
        IDataSet ACTUAL = new FilteredDataSet(filter, dbUnitCon.createDataSet());

        Assertion.assertEquals(EXPECTED, ACTUAL);
    }

}
