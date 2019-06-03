package org.noise_planet.roademission

import com.opencsv.CSVWriter
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.SourceURI
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.csv.CSVDriverFunction
import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.functions.io.shp.SHPWrite
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.RootProgressVisitor
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap
import java.nio.file.Path
import java.nio.file.Paths
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.DriverManager
import java.text.DateFormat
import java.text.DecimalFormat
import java.text.SimpleDateFormat

/**
 * To run
 * Just type "gradlew -Pworkdir=out/"
 */
@CompileStatic
class Main {
    static void main(String[] args) {

        // Name of the repository for the study area
        String zone_name = "my_zone"   // "zone_capteur"

        // Names of the Input tables (.shp) and their attributes
        String in_building_table_name = "buildings_zone"
        String in_area_table_name = "zone"
        String in_receivers_table_name = "receivers"
        String in_traffic_table_name = "roads_traffic_zone_format"
        String in_land_use_table_name = "land_use_zone"
        String in_topo_table_name = "dem_zone"

        // Names of the NoiseModelling tables (.shp) and their attributes
        String nm_building_table_name = "buildings"
        String height_field_name = "height"
        String nm_area_table_name = "study_area"
        String nm_receivers_table_name = "receivers"
        String nm_traffic_table_name = "roads"
        String nm_land_use_table_name = "ground_type"
        String nm_topo_table_name = "topography"
        String nm_receivers_att_day_table = "receivers_att_day_zone"
        String nm_receiver_lvl_day_table = "receivers_lvl_day_zone"
        String nm_receivers_att_evening_table = "receivers_att_evening_zone"
        String nm_receiver_lvl_evening_table = "receivers_lvl_evening_zone"
        String nm_receivers_att_night_table = "receivers_att_night_zone"
        String nm_receiver_lvl_night_table = "receivers_lvl_night_zone"

        // Propagation parameters
        double max_src_dist = 250
        double max_ref_dist = 250
        int reflexion_order = 1
        double wall_alpha = 0.1
        boolean compute_vertical_diffraction = true
        boolean compute_horizontal_diffraction = true
        // double[] favrose = [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00]  // rose of favourable conditions
        int n_thread = 10

        // Full paths to yhe scripts ans the project
        @SourceURI
        URI sourceUri
        Path scriptLocation = Paths.get(sourceUri)
        String projectPath = scriptLocation.getParent().getParent().getParent().getParent().getParent().getParent().getParent()

        // Init output logger
        Logger logger = LoggerFactory.getLogger(Main.class)
        logger.info(String.format("Working system  information: "))
        logger.info(String.format("\t| Working directory: %s", new File(projectPath).getAbsolutePath()))
        logger.info(String.format("\t| Total memory (bytes): %s", Runtime.getRuntime().totalMemory()))
        logger.info(String.format("\t| Java version: %s", System.getProperty("java.version")))

        // Date for the name of the results repository
        Date the_date = new Date()
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")

        // Full paths to input and output repositories
        String inputPath = projectPath + "/InputFiles/" + zone_name + "/"
        String outputRootPath = projectPath + "/Results/"

        // Init csv driver
        CSVDriverFunction csv = new CSVDriverFunction()

        // Création du dossier de résultats s'ils n'existe pas
        String outputPath = outputRootPath + "/" + zone_name + '_' + formatter.format(the_date) + "/"
        File dataOutDir = new File(outputPath)
        dataOutDir.mkdir()

        // Create spatial database
        String databasePath = projectPath+"database"
        String dbName = new File(databasePath).toURI()
//        ArrayList<Connection> connections = new ArrayList<>()
//        Connection connection = new ConnectionWrapper(DriverManager.getConnection("jdbc:h2:/"+databasePath+";DB_CLOSE_DELAY=30;DEFRAG_ALWAYS=TRUE", "sa", ""))
//        connections.add(connection)
//        def sql = Sql.newInstance("jdbc:h2:/"+databasePath+";DB_CLOSE_DELAY=30;DEFRAG_ALWAYS=TRUE", "sa", "", 'org.h2.Driver')
//        sql.execute("create alias if not exists h2gis_spatial for \"org.h2gis.functions.factory.H2GISFunctions.load\";")
//        sql.execute("call h2gis_spatial();")
        Connection connection = SFSUtilities.wrapConnection(DbUtilities.createSpatialDataBase(dbName, true))
        Sql sql = new Sql(connection)

        logger.info(String.format("Import data: "))

        // Evaluate receiver points using provided buildings
        sql.execute("DROP TABLE IF EXISTS "+nm_building_table_name.toUpperCase()+";")
        logger.info("\t| Read buildings file")
        SHPRead.readShape(connection, inputPath+in_building_table_name+".shp", nm_building_table_name.toUpperCase())
        SHPRead.readShape(connection, inputPath+in_area_table_name+".shp", nm_area_table_name.toUpperCase())
        sql.execute("CREATE SPATIAL INDEX ON "+nm_building_table_name.toUpperCase()+"(THE_GEOM);")
        SHPWrite.exportTable(connection, outputPath+nm_building_table_name+".shp", nm_building_table_name.toUpperCase())
        logger.info("\t  -> Building file loaded")

        // Load or create receivers points
        sql.execute("DROP TABLE IF EXISTS "+nm_receivers_table_name.toUpperCase()+";")
        logger.info("\t| Read receivers file")
        SHPRead.readShape(connection, inputPath+in_receivers_table_name+".shp", nm_receivers_table_name.toUpperCase())
        sql.execute("CREATE SPATIAL INDEX ON "+nm_receivers_table_name.toUpperCase()+"(THE_GEOM);")
        SHPWrite.exportTable(connection, outputPath+nm_receivers_table_name+".shp", nm_receivers_table_name.toUpperCase())
        logger.info("\t  -> Receivers file loaded")

        // Load roads
        logger.info("\t| Read road geometries and traffic")
        SHPRead.readShape(connection, inputPath+in_traffic_table_name+".shp", nm_traffic_table_name.toUpperCase()+"2")
        sql.execute("DROP TABLE IF EXISTS "+nm_traffic_table_name.toUpperCase()+";")
        sql.execute("CREATE TABLE "+nm_traffic_table_name.toUpperCase()+" AS SELECT id, ST_UpdateZ(THE_GEOM, 0.05) AS the_geom, \n" +
                    "lv_d_speed, mv_d_speed, hv_d_speed, wav_d_spee, wbv_d_spee,\n" +
                    "lv_e_speed, mv_e_speed, hv_e_speed, wav_e_spee, wbv_e_spee,\n" +
                    "lv_n_speed, mv_n_speed, hv_n_speed, wav_n_spee, wbv_n_spee,\n" +
                    "lv_d_per_h, ml_d_per_h, hv_d_per_h, wa_d_per_h, wb_d_per_h,\n" +
                    "lv_e_per_h, ml_e_per_h, hv_e_per_h, wa_e_per_h, wb_e_per_h,\n" +
                    "lv_n_per_h, ml_n_per_h, hv_n_per_h, wa_n_per_h, wb_n_per_h,\n" +
                    "Zstart, Zend, road_pav from "+nm_traffic_table_name.toUpperCase()+"2;")
        sql.execute("ALTER TABLE "+nm_traffic_table_name.toUpperCase()+" ALTER COLUMN ID SET NOT NULL;")
        sql.execute("ALTER TABLE "+nm_traffic_table_name.toUpperCase()+" ADD PRIMARY KEY (ID);")
        sql.execute("CREATE SPATIAL INDEX ON "+nm_traffic_table_name.toUpperCase()+"(THE_GEOM);")
        SHPWrite.exportTable(connection, outputPath+nm_traffic_table_name+".shp", nm_traffic_table_name.toUpperCase())
        logger.info("\t  -> Road file loaded")

        // Load ground type
        logger.info("\t| Read ground surface categories")
        SHPRead.readShape(connection, inputPath+in_land_use_table_name+".shp", nm_land_use_table_name.toUpperCase())
        sql.execute("CREATE SPATIAL INDEX ON "+nm_land_use_table_name.toUpperCase()+"(THE_GEOM);")
        SHPWrite.exportTable(connection, outputPath+nm_land_use_table_name+".shp", nm_land_use_table_name.toUpperCase())
        logger.info("\t  -> Surface categories file loaded")

        // Load Topography
        logger.info("\t| Read topography")
        SHPRead.readShape(connection, inputPath+in_topo_table_name+".shp", "DEM")
        sql.execute("DROP TABLE TOPOGRAPHY if exists;")
        sql.execute("CREATE TABLE "+nm_topo_table_name.toUpperCase()+" AS SELECT ST_UpdateZ(THE_GEOM, CONTOUR) AS the_geom from DEM;")
        sql.execute("CREATE SPATIAL INDEX ON "+nm_topo_table_name.toUpperCase()+"(THE_GEOM);")
        SHPWrite.exportTable(connection, outputPath+nm_topo_table_name+".shp", nm_topo_table_name.toUpperCase())
        logger.info("\t  -> Topography file loaded")

        logger.info("Computations")
        logger.info("\t| NoiseModelling initialization...")
        // Init NoiseModelling
        PointNoiseMap pointNoiseMap = new PointNoiseMap(nm_building_table_name.toUpperCase(), nm_traffic_table_name.toUpperCase(), nm_receivers_table_name.toUpperCase())
        pointNoiseMap.setSoilTableName(nm_land_use_table_name.toUpperCase())
        pointNoiseMap.setDemTable(nm_topo_table_name.toUpperCase())
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
        storageFactory.setWorkingDir(outputPath)
        logger.info("\t  -> NoiseModelling initialized")
        
        logger.info("\t| Compute source-receiver rays and related Lden and Ln...")
        List<ComputeRaysOut.verticeSL> allSoundLevels = new ArrayList<>()
        try {
            storageFactory.openPathOutputFile(new File(outputPath, "rays.gz").absolutePath)
            RootProgressVisitor progressLogger = new RootProgressVisitor(2, true, 1)
            pointNoiseMap.initialize(connection, progressLogger)
            progressLogger.endStep()
            // Set of already processed receivers
            Set<Long> receivers = new HashSet<>()
            ProgressVisitor progressVisitor = progressLogger.subProcess(pointNoiseMap.getGridDim()*pointNoiseMap.getGridDim())
            for (int i = 0; i < pointNoiseMap.getGridDim(); i++) {
                for (int j = 0; j < pointNoiseMap.getGridDim(); j++) {
                    logger.info("\t    ... compute i=" + i.toString() + ", j= " +j.toString() )
                    IComputeRaysOut out = pointNoiseMap.evaluateCell(connection, i, j, progressVisitor, receivers)
                    if (out instanceof ComputeRaysOut) {
                        allSoundLevels.addAll(((ComputeRaysOut) out).getVerticesSoundLevel())
                    }
                }
            }
            logger.info("\t  -> Source-receiver rays and related Lden and Ln computed")

            logger.info("\t| Compute global sound levels at receivers...")
            Map<Integer, double[]> recSoundLevels = new HashMap<>()
            for (int i=0;i< allSoundLevels.size() ; i++) {
                int idReceiver = (Integer) allSoundLevels.get(i).receiverId
                int idSource = (Integer) allSoundLevels.get(i).sourceId
                double[] recSoundLevel = allSoundLevels.get(i).value
                if (!Double.isNaN(recSoundLevel[0])) {
                    if (recSoundLevels.containsKey(idReceiver)) {
                        recSoundLevel = ComputeRays.sumDbArray(recSoundLevel, recSoundLevels.get(idReceiver))
                        recSoundLevels.replace(idReceiver, recSoundLevel)
                    } else {
                        recSoundLevels.put(idReceiver, recSoundLevel)
                    }
                }
            }
//            for (int i=0;i< allSoundLevels.size() ; i++) {
//                int idReceiver = (Integer) allSoundLevels.get(i).receiverId
//                int idSource = (Integer) allSoundLevels.get(i).sourceId
//                List<PropagationPath> propagationPaths = new ArrayList<>()
//                double[] recSoundLevel = allSoundLevels.get(i).value
//                if (recSoundLevels.containsKey(idReceiver)) {
//                    recSoundLevel = ComputeRays.sumDbArray(recSoundLevel, recSoundLevels.get(idReceiver))
//                    recSoundLevels.replace(idReceiver, recSoundLevel)
//                } else {
//                    recSoundLevels.put(idReceiver, recSoundLevel)
//                }
//            }
            logger.info("\t  -> Global sound levels at receivers computed")

            logger.info("\t| Write Lden to csv file...")
            CSVWriter csvWriter = new CSVWriter(new FileWriter(outputPath+"/ReceiversLden.csv"))
            for (Map.Entry<Integer, double[]> entry : recSoundLevels.entrySet()) {
                Integer key = entry.getKey()
                double[] value = entry.getValue()
                csvWriter.writeNext([key.toString(), ComputeRays.wToDba(ComputeRays.sumArray(ComputeRays.dbaToW(value))).toString()] as String[])
            }
            // closing csvWriter connection
            csvWriter.close()
            logger.info("\t  -> csv file generated")

//            // Sound levels at receivers
//            sql.execute("DROP TABLE "+nm_receivers_att_day_table+", "+nm_receiver_lvl_evening_table+", "+nm_receiver_lvl_night_table+" IF EXISTS;")
//            sql.execute("CREATE TABLE "+nm_receivers_att_day_table+"(idrecepteur integer, idsource integer, \n"+
//                        "att63 double precision, att125 double precision, att250 double precision, \n"+
//                        "att500 double precision, att1000 double precision, att2000 double precision, \n"+
//                        "att4000 double precision, att8000 double precision);")
//            // @todo : idem avec receivers_att_evening_table et receivers_att_night_table
//            def qry = "INSERT INTO "+nm_receivers_att_day_table+"(idrecepteur, idsource,"+
//                      "att63, att125, att250, att500, att1000, att2000, att4000, att8000) "+
//                      "VALUES (?,?,?,?,?,?,?,?,?,?);"
//            sql.withBatch(100, qry) { ps ->
//                for (int i=0;i< allAttenuations.size() ; i++) {
//                    ps.addBatch(allAttenuations.get(i).receiverId, allAttenuations.get(i).sourceId,
//                                allAttenuations.get(i).value[0], allAttenuations.get(i).value[1],
//                                allAttenuations.get(i).value[2], allAttenuations.get(i).value[3],
//                                allAttenuations.get(i).value[4], allAttenuations.get(i).value[5],
//                                allAttenuations.get(i).value[6], allAttenuations.get(i).value[7])
//                }
//            }
//            sql.execute('create table '+nm_receivers_att_evening_table+' as select * from '+nm_receivers_att_day_table+';')
//            sql.execute('create table '+nm_receivers_att_night_table+' as select * from '+nm_receivers_att_day_table+';')
//
//            csv.exportTable(connection, nm_receivers_att_day_table, new File(outputPath+nm_receivers_att_day_table+".csv"), new EmptyProgressVisitor())

        } finally {
            storageFactory.closeWriteThread()
        }

    }
}
