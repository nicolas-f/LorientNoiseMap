package org.noise_planet.roademission

import com.opencsv.CSVWriter
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.utilities.SFSUtilities
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.RootProgressVisitor
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.text.DateFormat
import java.text.DecimalFormat
import java.text.SimpleDateFormat

/**
 * To run
 * Just type "gradlew -Pworkdir=out/"
 */
@CompileStatic
class MainPrev {
    static void main(String[] args) {

    /**
     //////////////////////
     // Input parameters //
     //////////////////////
     */

        // Names of the Input tables (.shp) and their attributes
        String in_building_table_name = "buildings_zone_capteur"
        String in_area_table_name = "zone_capteur_2154"
        String in_receivers_table_name = "RecepteursQuestionnaire"
        String in_traffic_table_name = "roads_traffic_zone_capteur_lite"

        // Names of the NoiseModelling tables (.shp) and their attributes
        String nm_building_table_name = "buildings"
        String height_field_name = "hauteur"
        String nm_area_table_name = "study_area"
        String nm_receivers_table_name = "receivers"
        String nm_traffic_table_name = "roads"
        String soil_table_name = "ground_type"
        String topo_table_name = "topography"

        // Paramètres de propagation
        double max_src_dist = 250
        double max_ref_dist = 250
        int reflexion_order = 1
        double wall_alpha = 0.1
        boolean compute_vertical_diffraction = true
        boolean compute_horizontal_diffraction = true
        // double[] favrose = [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00]  // rose of favourable conditions
        int n_thread = 10

        // Read working directory argument
        String workingDir = ""
        if (args.length > 0) {
            workingDir = args[0]
        }

        // Init output logger
        Logger logger = LoggerFactory.getLogger(Main.class)
        logger.info(String.format("Working directory is %s", new File(workingDir).getAbsolutePath()))

        // Create spatial database
        String dbName = new File(workingDir + "database").toURI()
        Connection connection = SFSUtilities.wrapConnection(DbUtilities.createSpatialDataBase(dbName, true))
        Sql sql = new Sql(connection)

        // Evaluate receiver points using provided buildings
        sql.execute("DROP TABLE IF EXISTS "+nm_building_table_name.toUpperCase())
        logger.info("Read buildings file")
        SHPRead.readShape(connection, "data/"+in_building_table_name.toUpperCase()+".shp", nm_building_table_name.toUpperCase())
        SHPRead.readShape(connection, "data/"+in_area_table_name.toUpperCase()+".shp", nm_area_table_name.toUpperCase())
        sql.execute("CREATE SPATIAL INDEX ON "+nm_building_table_name.toUpperCase()+"(THE_GEOM)")
        logger.info("Building file loaded")

        // Load or create receivers points
        sql.execute("DROP TABLE IF EXISTS "+nm_receivers_table_name.toUpperCase())
        logger.info("Read receivers file")
        SHPRead.readShape(connection, "data/"+in_receivers_table_name.toUpperCase()+".shp", nm_receivers_table_name.toUpperCase())
        sql.execute("CREATE SPATIAL INDEX ON "+nm_receivers_table_name.toUpperCase()+"(THE_GEOM)")

        // Load roads
        logger.info("Read road geometries and traffic")
        SHPRead.readShape(connection, "data/"+in_traffic_table_name.toUpperCase()+".shp", "ROADS2")
        sql.execute("DROP TABLE "+nm_traffic_table_name.toUpperCase()+" if exists;")
        sql.execute('CREATE TABLE ROADS AS SELECT id, ST_UpdateZ(THE_GEOM, 0.05) the_geom, \n' +
                        'lv_d_speed, mv_d_speed, hv_d_speed, wav_d_spee, wbv_d_spee,\n' +
                        'lv_e_speed, mv_e_speed, hv_e_speed, wav_e_spee, wbv_e_spee,\n' +
                        'lv_n_speed, mv_n_speed, hv_n_speed, wav_n_spee, wbv_n_spee,\n' +
                        'vl_d_per_h, ml_d_per_h, pl_d_per_h, wa_d_per_h, wb_d_per_h,\n' +
                        'vl_e_per_h, ml_e_per_h, pl_e_per_h, wa_e_per_h, wb_e_per_h,\n' +
                        'vl_n_per_h, ml_n_per_h, pl_n_per_h, wa_n_per_h, wb_n_per_h,\n' +
                        'Zstart, Zend, Juncdist, Junc_type, road_pav FROM ROADS2;')
        sql.execute("ALTER TABLE "+nm_traffic_table_name.toUpperCase()+" ALTER COLUMN ID SET NOT NULL;")
        sql.execute("ALTER TABLE "+nm_traffic_table_name.toUpperCase()+" ADD PRIMARY KEY (ID);")
        sql.execute("CREATE SPATIAL INDEX ON "+nm_traffic_table_name.toUpperCase()+"(THE_GEOM)")
        logger.info("Road file loaded")

        // Load ground type
        logger.info("Read ground surface categories")
        SHPRead.readShape(connection, "data/land_use_zone_capteur4.shp", "GROUND_TYPE")
        sql.execute("CREATE SPATIAL INDEX ON GROUND_TYPE(THE_GEOM)")
        logger.info("Surface categories file loaded")

        // Load Topography
        logger.info("Read topography")
        SHPRead.readShape(connection, "data/DEM_LITE.shp", "DEM")
        sql.execute("DROP TABLE TOPOGRAPHY if exists;")
        sql.execute("CREATE TABLE TOPOGRAPHY AS SELECT ST_UpdateZ(THE_GEOM, CONTOUR) the_geom from DEM;")
        sql.execute("CREATE SPATIAL INDEX ON TOPOGRAPHY(THE_GEOM)")
        logger.info("Topography file loaded")

        // Init NoiseModelling
        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS", "ROADS", "RECEIVERS")
        pointNoiseMap.setSoilTableName(soil_table_name.toUpperCase())
        pointNoiseMap.setDemTable(topo_table_name.toUpperCase())
        pointNoiseMap.setMaximumPropagationDistance(max_src_dist)
        pointNoiseMap.setMaximumReflectionDistance(max_ref_dist)
        pointNoiseMap.setSoundReflectionOrder(reflexion_order)
        pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction)
        pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction)
        pointNoiseMap.setWallAbsorption(wall_alpha)
        pointNoiseMap.setHeightField(height_field_name.toUpperCase())
        pointNoiseMap.setThreadCount(n_thread) // Use 4 cpu threads
        //pointNoiseMap.setMaximumError(0.0d)
        PropagationPathStorageFactory storageFactory = new PropagationPathStorageFactory()
        TrafficPropagationProcessDataFactory trafficPropagationProcessDataFactory = new TrafficPropagationProcessDataFactory()
        pointNoiseMap.setPropagationProcessDataFactory(trafficPropagationProcessDataFactory)
        pointNoiseMap.setComputeRaysOutFactory(storageFactory)
        storageFactory.setWorkingDir(workingDir)

        List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>()
        try {
            storageFactory.openPathOutputFile(new File(workingDir, "rays.gz").absolutePath)
            RootProgressVisitor progressLogger = new RootProgressVisitor(2, true, 1)
            pointNoiseMap.initialize(connection, progressLogger)
            progressLogger.endStep()
            // Set of already processed receivers
            Set<Long> receivers = new HashSet<>()
            ProgressVisitor progressVisitor = progressLogger.subProcess(pointNoiseMap.getGridDim()*pointNoiseMap.getGridDim())
            for (int i = 0; i < pointNoiseMap.getGridDim(); i++) {
                for (int j = 0; j < pointNoiseMap.getGridDim(); j++) {
                    logger.info("Compute... i:" + i.toString() + " j: " +j.toString() )
                    IComputeRaysOut out = pointNoiseMap.evaluateCell(connection, i, j, progressVisitor, receivers)
                    if (out instanceof ComputeRaysOut) {
                        allLevels.addAll(((ComputeRaysOut) out).getVerticesSoundLevel())
                    }
                }
            }

            logger.info("Compute results by receivers...")
            Map<Integer, double[]> soundLevels = new HashMap<>()
            for (int i=0;i< allLevels.size() ; i++) {
                int idReceiver = (Integer) allLevels.get(i).receiverId
                int idSource = (Integer) allLevels.get(i).sourceId
                double[] soundLevel = allLevels.get(i).value
                if (!Double.isNaN(soundLevel[0])) {
                    if (soundLevels.containsKey(idReceiver)) {
                        soundLevel = ComputeRays.sumDbArray(soundLevel, soundLevels.get(idReceiver))
                        soundLevels.replace(idReceiver, soundLevel)
                    } else {
                        soundLevels.put(idReceiver, soundLevel)
                    }
                }
            }
//            for (int i=0;i< allLevels.size() ; i++) {
//                int idReceiver = (Integer) allLevels.get(i).receiverId
//                int idSource = (Integer) allLevels.get(i).sourceId
//                List<PropagationPath> propagationPaths = new ArrayList<>()
//                double[] soundLevel = allLevels.get(i).value
//                if (soundLevels.containsKey(idReceiver)) {
//                    soundLevel = ComputeRays.sumDbArray(soundLevel, soundLevels.get(idReceiver))
//                    soundLevels.replace(idReceiver, soundLevel)
//                } else {
//                    soundLevels.put(idReceiver, soundLevel)
//                }
//            }

            logger.info("Write results to csv file...")
            CSVWriter writer = new CSVWriter(new FileWriter(workingDir + "/Resultat.csv"))
            for (Map.Entry<Integer, double[]> entry : soundLevels.entrySet()) {
                Integer key = entry.getKey()
                double[] value = entry.getValue()
                writer.writeNext([key.toString(), ComputeRays.wToDba(ComputeRays.sumArray(ComputeRays.dbaToW(value))).toString()] as String[])
            }
            // closing writer connection
            writer.close()


            /*sql.execute("drop table if exists receiver_lvl_day_zone, receiver_lvl_evening_zone, receiver_lvl_night_zone;")
            sql.execute("create table receiver_lvl_day_zone (idrecepteur integer, idsource integer,att63 double precision, att125 double precision, att250 double precision, att500 double precision, att1000 double precision, att2000 double precision, att4000 double precision, att8000 double precision);")

            def qry = 'INSERT INTO RECEIVER_LVL_DAY_ZONE (IDRECEPTEUR, IDSOURCE,' +
                    'ATT63, ATT125, ATT250, ATT500, ATT1000,ATT2000, ATT4000, ATT8000) ' +
                    'VALUES (?,?,?,?,?,?,?,?,?,?);'
            sql.withBatch(100, qry) { ps ->
                for (int i=0;i< allLevels.size() ; i++) {
                    ps.addBatch(allLevels.get(i).receiverId, allLevels.get(i).sourceId,
                            allLevels.get(i).value[0], allLevels.get(i).value[1], allLevels.get(i).value[2],
                            allLevels.get(i).value[3], allLevels.get(i).value[4], allLevels.get(i).value[5],
                            allLevels.get(i).value[6], allLevels.get(i).value[7])
                    ps
                }
            }*/
        } finally {
            storageFactory.closeWriteThread()
        }

    }
}
