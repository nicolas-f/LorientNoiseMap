package org.noise_planet.roademission

import com.opencsv.CSVWriter
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2.table.Table
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.utilities.SFSUtilities
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.propagation.*
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.zip.GZIPInputStream

/**
 * To run
 * Just type "gradlew -Pworkdir=out2/"
 */
@CompileStatic
class MainSA {
    public static SensitivityProcessData sensitivityProcessData = new SensitivityProcessData()

    static void main(String[] args) {
        // Read working directory argument
        String workingDir = ""
        if (args.length > 0) {
            workingDir = args[0]
        }

        // Init output logger
        Logger logger = LoggerFactory.getLogger(MainSA.class)
        logger.info(String.format("Working directory is %s", new File(workingDir).getAbsolutePath()))

        // Create spatial database
        //TimeZone tz = TimeZone.getTimeZone("UTC")
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.getDefault())

        //df.setTimeZone(tz)
        String dbName = new File(workingDir + "database2").toURI()
        Connection connection = SFSUtilities.wrapConnection(DbUtilities.createSpatialDataBase(dbName, true))
        Sql sql = new Sql(connection)


        int nvar = 0 // pas toucher
        int nr = 0 // pas toucher
        int nSimu = 0 // ne pas toucher
        int n_comp = 0 // pas toucher
        int i_read = 1   //nombre de lignes d'entête

        // copier les fichiers dans le repertoire Results
        File source = new File("D:\\aumond\\Documents\\CENSE\\WP2\\Analyses\\Incertitudes\\Incertitudes\\Config.csv")
        File dest = new File(workingDir + "\\Config.csv")
        copyFileUsingStream(source, dest)
        File source2 = new File("D:\\aumond\\Documents\\CENSE\\WP2\\Analyses\\Incertitudes\\Incertitudes\\Exp_comp1m.csv")
        File dest2 = new File(workingDir + "\\Exp_comp1m.csv")
        copyFileUsingStream(source2, dest2)

        // lire les 4 premieres lignes de config
        new File("D:\\aumond\\Documents\\CENSE\\WP2\\Analyses\\Incertitudes\\Incertitudes\\Config.csv").splitEachLine(",") {
            fields ->
                switch (i_read) {
                    case 1:
                        nvar = fields[0].toInteger()
                    case 2:
                        nr = fields[0].toInteger()
                    case 3:
                        nSimu = fields[0].toInteger()
                    case 4:
                        n_comp = fields[0].toInteger()
                    default: break
                }
                i_read = i_read + 1
        }

        // Evaluate receiver points using provided buildings

        sql.execute("DROP TABLE IF EXISTS BUILDINGS")

        // Load roads
        logger.info("Read road geometries and traffic")
        SHPRead.readShape(connection, "data/ROADS_TRAFFIC_ZONE_CAPTEUR_LITE.shp", "ROADS2")
        sql.execute("DROP TABLE ROADS if exists;")
        sql.execute('CREATE TABLE ROADS AS SELECT id, ST_UpdateZ(THE_GEOM, 0.05) the_geom, \n' +
                'lv_d_speed,mv_d_speed,hv_d_speed,wav_d_spee,wbv_d_spee,\n' +
                'lv_e_speed,mv_e_speed,hv_e_speed,wav_e_spee,wbv_e_spee,\n' +
                'lv_n_speed,mv_n_speed,hv_n_speed,wav_n_spee,wbv_n_spee,\n' +
                'vl_d_per_h,ml_d_per_h,pl_d_per_h,wa_d_per_h,wb_d_per_h,\n' +
                'vl_e_per_h,ml_e_per_h,pl_e_per_h,wa_e_per_h,wb_e_per_h,\n' +
                'vl_n_per_h,ml_n_per_h,pl_n_per_h,wa_n_per_h,wb_n_per_h,\n' +
                'Zstart,Zend, Juncdist, Junc_type,road_pav FROM ROADS2;')
        sql.execute('ALTER TABLE ROADS ALTER COLUMN ID SET NOT NULL;')
        sql.execute('ALTER TABLE ROADS ADD PRIMARY KEY (ID);')
        sql.execute("CREATE SPATIAL INDEX ON ROADS(THE_GEOM)")
        logger.info("Road file loaded")


        List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>()



        PropagationProcessPathData genericMeteoData = new PropagationProcessPathData()
        sensitivityProcessData.setSensitivityTable(dest2)

        int GZIP_CACHE_SIZE = (int) Math.pow(2, 19)

        try {
            FileInputStream fileInputStream = new FileInputStream(new File(workingDir, "rays.gz").getAbsolutePath())
            try {
                GZIPInputStream gzipInputStream = new GZIPInputStream((fileInputStream), GZIP_CACHE_SIZE)
                DataInputStream dataInputStream = new DataInputStream(gzipInputStream)
                System.out.println("Read file and apply sensitivity analysis")
                int oldIdReceiver = -1
                int oldIdSource = -1

                FileWriter csvFile = new FileWriter(new File(workingDir, "simu.csv"))
                List<double[]> simuSpectrum = new ArrayList<>()

                Map<Integer, List<double[]>>  sourceLevel = new HashMap<>()

                System.out.println("Prepare Sources")
                def timeStart = System.currentTimeMillis()

                sourceLevel = sensitivityProcessData.getTrafficLevel("ROADS2", sql, nSimu)
                def timeStart2 = System.currentTimeMillis()
                System.out.println(timeStart2-timeStart)

                System.out.println("Run SA")
                while (fileInputStream.available() > 0) {

                    PointToPointPaths paths = new PointToPointPaths()
                    paths.readPropagationPathListStream(dataInputStream)
                    int idReceiver = (Integer) paths.receiverId
                    int idSource = (Integer) paths.sourceId

                    if (idReceiver!= oldIdReceiver) {
                        logger.info("Receiver: " + oldIdReceiver)
                        // Save old receiver values
                        if (oldIdReceiver!= -1) {
                            for (int r = 0; r < nSimu; ++r) {
                                csvFile.append(String.format("%d\t%d\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\n", r, idReceiver, simuSpectrum.get(r)[0], simuSpectrum.get(r)[1], simuSpectrum.get(r)[2], simuSpectrum.get(r)[3], simuSpectrum.get(r)[4], simuSpectrum.get(r)[5], simuSpectrum.get(r)[6], simuSpectrum.get(r)[7]))
                            }
                        }
                        // Create new receiver value
                        simuSpectrum.clear()
                        for (int r = 0; r < nSimu; ++r) {
                            simuSpectrum.add(new double[PropagationProcessPathData.freq_lvl.size()])
                        }
                    }
                    oldIdReceiver = idReceiver
                    ComputeRaysOut out = new ComputeRaysOut(false, sensitivityProcessData.getGenericMeteoData(0))
                    for (int r = 0; r < nSimu; ++r) {
                        double[] attenuation = out.computeAttenuation(sensitivityProcessData.getGenericMeteoData(r), idSource, paths.getLi(), idReceiver, paths.propagationPathList)
                        double[] soundLevel = sumArray(attenuation, sourceLevel.get(idSource).get(r))
                        simuSpectrum[r] = ComputeRays.sumDbArray(simuSpectrum[r],soundLevel)
                    }

                }
                csvFile.close()

            } finally {

                fileInputStream.close()
            }

            /*for (int r = 0; r < nSimu; ++r) {
                System.out.println(100 * r / nSimu + "%")

                for (int i = 0; i < allLevels.size(); i++) {
                    int idReceiver = (Integer) allLevels.get(i).receiverId
                    int idSource = (Integer) allLevels.get(i).sourceId
                    List<PropagationPath> propagationPaths = new ArrayList<>()
                    double[] soundLevel = allLevels.get(i).value
                    if (soundLevels.containsKey(idReceiver)) {
                        soundLevel = ComputeRays.sumDbArray(soundLevel, soundLevels.get(idReceiver))
                        soundLevels.replace(idReceiver, soundLevel)
                    } else {
                        soundLevels.put(idReceiver, soundLevel)
                    }
                }

            }*/


        }

        finally {

        }

    }


    private static double[] sumArray(double[] array1, double[] array2) {
        if (array1.length != array2.length) {
            throw new IllegalArgumentException("Not same size array")
        } else {
            double[] sum = new double[array1.length]

            for (int i = 0; i < array1.length; ++i) {
                sum[i] = array1[i] + array2[i]
            }

            return sum
        }
    }
// fonction pour Copier les fichiers dans un autre répertoire
    private static void copyFileUsingStream(File source, File dest) throws IOException {
        InputStream is = null
        OutputStream os = null
        try {
            is = new FileInputStream(source)
            os = new FileOutputStream(dest)
            byte[] buffer = new byte[1024]
            int length
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length)
            }
        } finally {
            is.close()
            os.close()
        }
    }

}
