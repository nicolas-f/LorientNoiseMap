package org.noise_planet.roademission

import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.BatchingStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2.Driver
import org.h2gis.functions.factory.H2GISFunctions
import org.h2gis.functions.io.shp.SHPWrite
import org.h2gis.functions.spatial.convert.ST_Force3D
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.LineString
import org.locationtech.jts.geom.Polygon
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.JTSUtility
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

class DbUtilities {


    private static String getDataBasePath(String dbName) {
        return dbName.startsWith("file:/") ? (new File(URI.create(dbName))).getAbsolutePath() : (new File(dbName)).getAbsolutePath()
    }


    static Connection createSpatialDataBase(String dbName, boolean initSpatial) throws SQLException, ClassNotFoundException {
        String dbFilePath = getDataBasePath(dbName);
        File dbFile = new File(dbFilePath + ".mv.db")

        String databasePath = "jdbc:h2:" + dbFilePath + ";LOCK_MODE=0;LOG=0;DB_CLOSE_DELAY=5"

        if (dbFile.exists()) {
            dbFile.delete()
        }

        dbFile = new File(dbFilePath + ".mv.db")
        if (dbFile.exists()) {
            dbFile.delete()
        }
        Driver.load()
        Connection connection = DriverManager.getConnection(databasePath, "sa", "sa")
        if (initSpatial) {
            H2GISFunctions.load(connection)
        }

        return connection
    }

    @CompileStatic
    static void createReceiversFromBuildings(Sql sql, String buildingName, String areaTable) {
        sql.execute("DROP TABLE IF EXISTS GLUED_BUILDINGS")
        sql.execute("CREATE TABLE GLUED_BUILDINGS AS SELECT ST_UNION(ST_ACCUM(ST_BUFFER(B.THE_GEOM, 2.0,'endcap=square join=bevel'))) the_geom FROM "+buildingName+" B, "+areaTable+" A WHERE A.THE_GEOM && B.THE_GEOM AND ST_INTERSECTS(A.THE_GEOM, B.THE_GEOM)")
        Logger logger = LoggerFactory.getLogger("test")
        sql.execute("DROP TABLE IF EXISTS RECEIVERS")
        sql.execute("CREATE TABLE RECEIVERS(pk serial, the_geom GEOMETRY)")
        boolean pushed = false
        sql.withTransaction {
            sql.withBatch("INSERT INTO receivers(the_geom) VALUES (ST_MAKEPOINT(:px, :py, :pz))") { BatchingPreparedStatementWrapper batch ->
                sql.eachRow("SELECT THE_GEOM FROM ST_EXPLODE('GLUED_BUILDINGS')") {
                    row ->
                        List<Coordinate> receivers = new ArrayList<>();
                        ComputeRays.splitLineStringIntoPoints((LineString) ST_Force3D.force3D(((Polygon) row["the_geom"]).exteriorRing), 5.0d, receivers)
                        for (Coordinate p : receivers) {
                            p.setOrdinate(2, 4.0d)
                            batch.addBatch([px:p.x, py:p.y, pz:p.z])
                            pushed = true
                        }

                }
                if(pushed) {
                    batch.executeBatch()
                    pushed = false
                }
            }
        }
        SHPWrite.exportTable(sql.getConnection(), "data/receivers.shp", "RECEIVERS")
        sql.execute("DROP TABLE GLUED_BUILDINGS")
    }
}
