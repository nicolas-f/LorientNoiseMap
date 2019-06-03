package org.noise_planet.roademission

import groovy.sql.Sql
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData

import java.sql.SQLException

/**
 * Read source database and compute the sound emission spectrum of roads sources
 */
class SensitivityProcessData {
    static public Map<Integer, double[]> soundSourceLevels = new HashMap<>()
    public static PropagationProcessPathData genericMeteoData = new PropagationProcessPathData()

    // Init des variables
    ArrayList<Integer> Simu = new ArrayList<Integer>() // numero simu
    ArrayList<Double> CalcTime = new ArrayList<Double>() // temps calcul

    ArrayList<Double> Deb_d_lv = new ArrayList<Double>()
    ArrayList<Double> Deb_d_mv = new ArrayList<Double>()
    ArrayList<Double> Deb_d_hv = new ArrayList<Double>()
    ArrayList<Double> Deb_d_wav = new ArrayList<Double>()
    ArrayList<Double> Deb_d_wbv = new ArrayList<Double>()

    ArrayList<Double> Deb_e_lv = new ArrayList<Double>()
    ArrayList<Double> Deb_e_mv = new ArrayList<Double>()
    ArrayList<Double> Deb_e_hv = new ArrayList<Double>()
    ArrayList<Double> Deb_e_wav = new ArrayList<Double>()
    ArrayList<Double> Deb_e_wbv = new ArrayList<Double>()

    ArrayList<Double> Deb_n_lv = new ArrayList<Double>()
    ArrayList<Double> Deb_n_mv = new ArrayList<Double>()
    ArrayList<Double> Deb_n_hv = new ArrayList<Double>()
    ArrayList<Double> Deb_n_wav = new ArrayList<Double>()
    ArrayList<Double> Deb_n_wbv = new ArrayList<Double>()

    ArrayList<Double> Speed_d_lv = new ArrayList<Double>()
    ArrayList<Double> Speed_d_mv = new ArrayList<Double>()
    ArrayList<Double> Speed_d_hv = new ArrayList<Double>()
    ArrayList<Double> Speed_d_wav = new ArrayList<Double>()
    ArrayList<Double> Speed_d_wbv = new ArrayList<Double>()

    ArrayList<Double> Speed_e_lv = new ArrayList<Double>()
    ArrayList<Double> Speed_e_mv = new ArrayList<Double>()
    ArrayList<Double> Speed_e_hv = new ArrayList<Double>()
    ArrayList<Double> Speed_e_wav = new ArrayList<Double>()
    ArrayList<Double> Speed_e_wbv = new ArrayList<Double>()

    ArrayList<Double> Speed_n_lv = new ArrayList<Double>()
    ArrayList<Double> Speed_n_mv = new ArrayList<Double>()
    ArrayList<Double> Speed_n_hv = new ArrayList<Double>()
    ArrayList<Double> Speed_n_wav = new ArrayList<Double>()
    ArrayList<Double> Speed_n_wbv = new ArrayList<Double>()

    ArrayList<Double> slopeZ = new ArrayList<Double>()

    ArrayList<Double> Tempd_year = new ArrayList<Double>()
    ArrayList<Double> Tempe_year = new ArrayList<Double>()
    ArrayList<Double> Tempn_year = new ArrayList<Double>()

    ArrayList<Double> R_Road = new ArrayList<Double>()

    ArrayList<Double> Junc_dist = new ArrayList<Double>()
    ArrayList<Double> R_Junc = new ArrayList<Double>()

    ArrayList<Double> Dist = new ArrayList<Double>()

    Map<Integer, List<double[]>> getTrafficLevel(String tablename, Sql sql, int nSimu) throws SQLException {
        /**
         * Vehicles category Table 3 P.31 CNOSSOS_EU_JRC_REFERENCE_REPORT
         * lv : Passenger cars, delivery vans ≤ 3.5 tons, SUVs , MPVs including trailers and caravans
         * mv: Medium heavy vehicles, delivery vans > 3.5 tons,  buses, touring cars, etc. with two axles and twin tyre mounting on rear axle
         * hgv: Heavy duty vehicles, touring cars, buses, with three or more axles
         * wav:  mopeds, tricycles or quads ≤ 50 cc
         * wbv:  motorcycles, tricycles or quads > 50 cc
         * @param lv_speed Average light vehicle speed
         * @param mv_speed Average medium vehicle speed
         * @param hgv_speed Average heavy goods vehicle speed
         * @param wav_speed Average light 2 wheels vehicle speed
         * @param wbv_speed Average heavy 2 wheels vehicle speed
         * @param lvPerHour Average light vehicle per hour
         * @param mvPerHour Average heavy vehicle per hour
         * @param hgvPerHour Average heavy vehicle per hour
         * @param wavPerHour Average heavy vehicle per hour
         * @param wbvPerHour Average heavy vehicle per hour
         * @param FreqParam Studied Frequency
         * @param Temperature Temperature (Celsius)
         * @param roadSurface roadSurface empty default, NL01 FR01 ..
         * @param Ts_stud A limited period Ts (in months) over the year where a average proportion pm of light vehicles are equipped with studded tyres and during .
         * @param Pm_stud Average proportion of vehicles equipped with studded tyres
         * @param Junc_dist Distance to junction
         * @param Junc_type Type of junction ((k = 1 for a crossing with traffic lights ; k = 2 for a roundabout)
         */
        //connect
        Map<Integer, List<double[]>> sourceLevel = new HashMap<>()

        // memes valeurs d e et n
        sql.eachRow('SELECT id, the_geom,\n' +
                'lv_d_speed,mv_d_speed,hv_d_speed,wav_d_spee,wbv_d_spee,\n' +
                'lv_e_speed,mv_e_speed,hv_e_speed,wav_e_spee,wbv_e_spee,\n' +
                'lv_n_speed,mv_n_speed,hv_n_speed,wav_n_spee,wbv_n_spee,\n' +
                'vl_d_per_h,ml_d_per_h,pl_d_per_h,wa_d_per_h,wb_d_per_h,\n' +
                'vl_e_per_h,ml_e_per_h,pl_e_per_h,wa_e_per_h,wb_e_per_h,\n' +
                'vl_n_per_h,ml_n_per_h,pl_n_per_h,wa_n_per_h,wb_n_per_h,\n' +
                'Zstart,Zend, Juncdist, Junc_type,road_pav FROM ' + tablename + ';') { row ->

            def list = [63, 125, 250, 500, 1000, 2000, 4000, 8000]

            double[] res_d = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            double[] res_e = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            double[] res_n = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            int id = (int) row[0]
            //System.out.println("Source :" + id)
            Geometry the_geom = row[1]
            def lv_d_speed = (double) row[2]
            def mv_d_speed = (double) row[3]
            def hv_d_speed = (double) row[4]
            def wav_d_speed = (double) row[5]
            def wbv_d_speed = (double) row[6]
            def lv_e_speed = (double) row[7]
            def mv_e_speed = (double) row[8]
            def hv_e_speed = (double) row[9]
            def wav_e_speed = (double) row[10]
            def wbv_e_speed = (double) row[11]
            def lv_n_speed = (double) row[12]
            def mv_n_speed = (double) row[13]
            def hv_n_speed = (double) row[14]
            def wav_n_speed = (double) row[15]
            def wbv_n_speed = (double) row[16]
            def vl_d_per_hour = (double) row[17]
            def ml_d_per_hour = (double) row[18]
            def pl_d_per_hour = (double) row[19]
            def wa_d_per_hour = (double) row[20]
            def wb_d_per_hour = (double) row[21]
            def vl_e_per_hour = (double) row[22]
            def ml_e_per_hour = (double) row[23]
            def pl_e_per_hour = (double) row[24]
            def wa_e_per_hour = (double) row[25]
            def wb_e_per_hour = (double) row[26]
            def vl_n_per_hour = (double) row[27]
            def ml_n_per_hour = (double) row[28]
            def pl_n_per_hour = (double) row[29]
            def wa_n_per_hour = (double) row[30]
            def wb_n_per_hour = (double) row[31]
            def Zstart = (double) row[32]
            def Zend = (double) row[33]
            def Juncdist = (double) row[34]
            def Junc_type = (int) row[35]
            def road_pav = (int) row[36]

            // Ici on calcule les valeurs d'emission par tronçons et par fréquence

            List<double[]> sourceLevel2 = new ArrayList<>()
            for (int r = 0; r < nSimu; ++r) {

                int kk=0
                for (f in list) {
                    // fois 0.5 car moitié dans un sens et moitié dans l'autre
                    RSParametersCnossos srcParameters_d = new RSParametersCnossos(lv_d_speed * Speed_d_lv[r], mv_d_speed * Speed_d_mv[r], hv_d_speed * Speed_d_hv[r], wav_d_speed * Speed_d_wav[r], wbv_d_speed * Speed_d_wbv[r],
                            vl_d_per_hour * Deb_d_lv[r] * 0.5, ml_d_per_hour * Deb_d_mv[r] * 0.5, pl_d_per_hour * Deb_d_hv[r] * 0.5, wa_d_per_hour * Deb_d_wav[r] * 0.5, wb_d_per_hour * Deb_d_wbv[r] * 0.5,
                            f, Tempd_year[r], "NL01", 0, 0, Juncdist * Junc_dist[r], Junc_type)
                    /* RSParametersCnossos srcParameters_e = new RSParametersCnossos(lv_e_speed * Speed_e_lv[r], mv_e_speed * Speed_e_mv[r], hv_e_speed * Speed_e_hv[r], wav_e_speed * Speed_e_wav[r], wbv_e_speed * Speed_e_wbv[r],
                        vl_e_per_hour * Deb_e_lv[r] * 0.5, ml_e_per_hour * Deb_e_mv[r] * 0.5, pl_e_per_hour * Deb_e_hv[r] * 0.5, wa_e_per_hour * Deb_e_wav[r] * 0.5, wb_e_per_hour * Deb_e_wbv[r] * 0.5,
                        f, Tempe_year[r], "NL01", 0, 0, Juncdist * Junc_dist[r], Junc_type)
                RSParametersCnossos srcParameters_n = new RSParametersCnossos(lv_n_speed * Speed_n_lv[r], mv_n_speed * Speed_n_mv[r], hv_n_speed * Speed_n_hv[r], wav_n_speed * Speed_n_wav[r], wbv_n_speed * Speed_n_wbv[r],
                        vl_n_per_hour * Deb_n_lv[r] * 0.5, ml_n_per_hour * Deb_n_mv[r] * 0.5, pl_n_per_hour * Deb_n_hv[r] * 0.5, wa_n_per_hour * Deb_n_wav[r] * 0.5, wb_n_per_hour * Deb_n_wbv[r] * 0.5,
                        f, Tempn_year[r], "NL01", 0, 0, Juncdist * Junc_dist[r], Junc_type)
*/
                    srcParameters_d.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))
                    //srcParameters_e.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))
                    //srcParameters_n.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))
                    res_d[kk] = EvaluateRoadSourceCnossos.evaluate(srcParameters_d)
                    //res_e[kk] = EvaluateRoadSourceCnossos.evaluate(srcParameters_e)
                    //res_n[kk] = EvaluateRoadSourceCnossos.evaluate(srcParameters_n)
                    srcParameters_d.setSlopePercentage(RSParametersCnossos.computeSlope(Zend, Zstart, the_geom.getLength()))
                    //srcParameters_e.setSlopePercentage(RSParametersCnossos.computeSlope(Zend, Zstart, the_geom.getLength()))
                    //srcParameters_n.setSlopePercentage(RSParametersCnossos.computeSlope(Zend, Zstart, the_geom.getLength()))
                    res_d[kk] = 10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_d) / 10) + Math.pow(10, res_d[kk] / 10))
                    //res_e[kk] = 10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_e) / 10) + Math.pow(10, res_e[kk] / 10))
                    //res_n[kk] = 10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_n) / 10) + Math.pow(10, res_n[kk] / 10))
                    kk++
                }
                sourceLevel2.add(res_d)
            }
            sourceLevel.put(id, sourceLevel2)
        }
    return sourceLevel
}

PropagationProcessPathData getGenericMeteoData(int r) {
    genericMeteoData.setHumidity(70)
    genericMeteoData.setTemperature(Tempd_year[r])
    return genericMeteoData
}


void setSensitivityTable(File file) {
    //////////////////////
    // Import file text
    //////////////////////
    int i_read = 0;
    // Remplissage des variables avec le contenu du fichier plan d'exp
    file.splitEachLine(",") { fields ->

        Deb_d_lv.add(fields[0].toFloat())
        Deb_d_mv.add(fields[1].toFloat())
        Deb_d_hv.add(fields[2].toFloat())
        Deb_d_wav.add(fields[3].toFloat())
        Deb_d_wbv.add(fields[4].toFloat())

        Deb_e_lv.add(fields[5].toFloat())
        Deb_e_mv.add(fields[6].toFloat())
        Deb_e_hv.add(fields[7].toFloat())
        Deb_e_wav.add(fields[8].toFloat())
        Deb_e_wbv.add(fields[9].toFloat())

        Deb_n_lv.add(fields[10].toFloat())
        Deb_n_mv.add(fields[11].toFloat())
        Deb_n_hv.add(fields[12].toFloat())
        Deb_n_wav.add(fields[13].toFloat())
        Deb_n_wbv.add(fields[14].toFloat())

        Speed_d_lv.add(fields[15].toFloat())
        Speed_d_mv.add(fields[16].toFloat())
        Speed_d_hv.add(fields[17].toFloat())
        Speed_d_wav.add(fields[18].toFloat())
        Speed_d_wbv.add(fields[19].toFloat())

        Speed_e_lv.add(fields[20].toFloat())
        Speed_e_mv.add(fields[21].toFloat())
        Speed_e_hv.add(fields[22].toFloat())
        Speed_e_wav.add(fields[23].toFloat())
        Speed_e_wbv.add(fields[24].toFloat())

        Speed_n_lv.add(fields[25].toFloat())
        Speed_n_mv.add(fields[26].toFloat())
        Speed_n_hv.add(fields[27].toFloat())
        Speed_n_wav.add(fields[28].toFloat())
        Speed_n_wbv.add(fields[29].toFloat())

        slopeZ.add(fields[30].toFloat())

        Tempd_year.add(fields[31].toFloat())
        Tempe_year.add(fields[32].toFloat())
        Tempn_year.add(fields[33].toFloat())

        R_Road.add(fields[34].toFloat())

        Junc_dist.add(fields[35].toFloat())
        R_Junc.add(fields[36].toFloat())

        Dist.add(fields[37].toFloat())

        Simu.add(fields[38].toInteger())

        i_read = i_read + 1
    }

}

}
