package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.h2gis.utilities.SpatialResultSet
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.FastObstructionTest
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData

import java.sql.SQLException

/**
 * Read source database and compute the sound emission spectrum of roads sources
 */
@CompileStatic
class TrafficPropagationProcessData extends PropagationProcessData {
    // Lden values
    protected List<double[]> wjSourcesD = new ArrayList<>()
    protected List<double[]> wjSourcesE = new ArrayList<>()
    protected List<double[]> wjSourcesN = new ArrayList<>()

//    private String TMJA_FIELD_NAME = "CUMUL_TRAF"
//    private String ROAD_CATEGORY_FIELD_NAME = "CLAS_ADM"
//    private
//    static double[] lv_hourly_distribution = [0.56, 0.3, 0.21, 0.26, 0.69, 1.8, 4.29, 7.56, 7.09, 5.5, 4.96, 5.04,
//                                              5.8, 6.08, 6.23, 6.67, 7.84, 8.01, 7.12, 5.44, 3.45, 2.26, 1.72, 1.12]
//    private
//    static double[] hv_hourly_distribution = [1.01, 0.97, 1.06, 1.39, 2.05, 3.18, 4.77, 6.33, 6.72, 7.32, 7.37, 7.4,
//                                              6.16, 6.22, 6.84, 6.74, 6.23, 4.88, 3.79, 3.05, 2.36, 1.76, 1.34, 1.07]
    private static final int LDAY_START_HOUR = 6
    private static final int LDAY_STOP_HOUR = 18
    private static final int LEVE_START_HOUR = 18
    private static final int LEVE_STOP_HOUR = 22
    private static final int LNIG_START_HOUR = 22
    private static final int LNIG_STOP_HOUR = 6
//    private static final double HV_PERCENTAGE = 0.1
//    //private Map<Integer, Double> class_to_speed = [1:130, 2:80,3:50,4:50,5:59,6:50,7:50]


    public TrafficPropagationProcessData(FastObstructionTest freeFieldFinder) {
        super(freeFieldFinder);
    }


    @Override
    public void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {

        super.addSource(pk, geom, rs)

        // Read average 24h traffic
        //double tmja = rs.getDouble(TMJA_FIELD_NAME)

        //130 km/h 1:Autoroute
        //80 km/h  2:Nationale
        //50 km/h  3:Départementale
        //50 km/h  4:Voirie CUN
        //50 km/h  5:Inconnu
        //50 km/h  6:Privée
        //50 km/h  7:Communale
        //int road_cat = rs.getInt(ROAD_CATEGORY_FIELD_NAME)

        /*int roadType;
        if(road_cat == 1) {
            roadType = 10
        } else if(road_cat == 2) {
            roadType = 42
        } else {
            roadType = 62
        }
        double speed_lv = 50
        if(road_cat == 1) {
            speed_lv = 120
        } else if(road_cat == 2) {
            speed_lv = 80
        }
*/
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
        // Compute day average level
        Geometry the_geom = rs.getGeometry("the_geom")
        double lv_d_speed = rs.getDouble("lv_d_speed")
        double mv_d_speed = rs.getDouble("mv_d_speed")
        double hv_d_speed = rs.getDouble("hv_d_speed")
        double wav_d_speed = rs.getDouble("wav_d_spee")
        double wbv_d_speed = rs.getDouble("wbv_d_spee")
        double lv_e_speed = rs.getDouble("lv_e_speed")
        double mv_e_speed = rs.getDouble("mv_e_speed")
        double hv_e_speed = rs.getDouble("hv_e_speed")
        double wav_e_speed = rs.getDouble("wav_e_spee")
        double wbv_e_speed = rs.getDouble("wbv_e_spee")
        double lv_n_speed =rs.getDouble("lv_n_speed")
        double mv_n_speed = rs.getDouble("mv_n_speed")
        double hv_n_speed = rs.getDouble("hv_n_speed")
        double wav_n_speed =rs.getDouble("wav_n_spee")
        double wbv_n_speed = rs.getDouble("wbv_n_spee")
        double vl_d_per_hour = rs.getDouble("lv_d_per_h")
        double ml_d_per_hour =rs.getDouble("ml_d_per_h")
        double pl_d_per_hour = rs.getDouble("hv_d_per_h")
        double wa_d_per_hour = rs.getDouble("wa_d_per_h")
        double wb_d_per_hour =rs.getDouble("wb_d_per_h")
        double vl_e_per_hour = rs.getDouble("lv_e_per_h")
        double ml_e_per_hour = rs.getDouble("ml_e_per_h")
        double pl_e_per_hour = rs.getDouble("hv_e_per_h")
        double wa_e_per_hour = rs.getDouble("wa_e_per_h")
        double wb_e_per_hour = rs.getDouble("wb_e_per_h")
        double vl_n_per_hour =rs.getDouble("lv_n_per_h")
        double ml_n_per_hour = rs.getDouble("ml_n_per_h")
        double pl_n_per_hour = rs.getDouble("hv_n_per_h")
        double wa_n_per_hour = rs.getDouble("wa_n_per_h")
        double wb_n_per_hour = rs.getDouble("wb_n_per_h")
        double Zstart = rs.getDouble("Zstart")
        double Zend =rs.getDouble("Zend")
        double Juncdist = 200   // rs.getDouble("Juncdist")
        int Junc_type = 1       // rs.getInt("Junc_type")
        int road_pav = rs.getInt("road_pav")

        double[] ld = new double[PropagationProcessPathData.freq_lvl.size()]
        double[] le = new double[PropagationProcessPathData.freq_lvl.size()]
        double[] ln = new double[PropagationProcessPathData.freq_lvl.size()]
//        double[] res_d = new double[PropagationProcessPathData.freq_lvl.size()]
//        double[] res_e = new double[PropagationProcessPathData.freq_lvl.size()]
//        double[] res_n = new double[PropagationProcessPathData.freq_lvl.size()]

        int idFreq  = 0
        for(int freq : PropagationProcessPathData.freq_lvl) {
            RSParametersCnossos srcParameters_d = new RSParametersCnossos(lv_d_speed, mv_d_speed, hv_d_speed, wav_d_speed, wbv_d_speed,
                    vl_d_per_hour , ml_d_per_hour , pl_d_per_hour , wa_d_per_hour , wb_d_per_hour ,
                    freq, 20.0d, "FR_R2", 0, 0, 200.0d, Junc_type)
            RSParametersCnossos srcParameters_e = new RSParametersCnossos(lv_e_speed, mv_e_speed, hv_e_speed, wav_e_speed, wbv_e_speed,
                    vl_e_per_hour , ml_e_per_hour , pl_e_per_hour , wa_e_per_hour , wb_e_per_hour ,
                    freq, 20.0d, "FR_R2", 0, 0, 200.0d, Junc_type)
            RSParametersCnossos srcParameters_n = new RSParametersCnossos(lv_n_speed, mv_n_speed, hv_n_speed, wav_n_speed, wbv_n_speed,
                    vl_n_per_hour , ml_n_per_hour , pl_n_per_hour , wa_n_per_hour , wb_n_per_hour ,
                    freq, 20.0d, "FR_R2", 0, 0, 200.0d, Junc_type)

            srcParameters_d.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))
            srcParameters_e.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))
            srcParameters_n.setSlopePercentage(RSParametersCnossos.computeSlope(Zstart, Zend, the_geom.getLength()))

            ld[idFreq] += ComputeRays.dbaToW(EvaluateRoadSourceCnossos.evaluate(srcParameters_d))
            le[idFreq] += ComputeRays.dbaToW(EvaluateRoadSourceCnossos.evaluate(srcParameters_e))
            ln[idFreq] += ComputeRays.dbaToW(EvaluateRoadSourceCnossos.evaluate(srcParameters_n))
//            res_d[idFreq ] = 10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_d) / 10) + Math.pow(10, res_d[idFreq] / 10))
//            res_e[idFreq ] = 10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_e) / 10) + Math.pow(10, res_e[idFreq] / 10))
//            res_n[idFreq ] = 10 * Math.log10(Math.pow(10, EvaluateRoadSourceCnossos.evaluate(srcParameters_n) / 10) + Math.pow(10, res_n[idFreq] / 10))
            idFreq++
        }

        // Average
        for(int i=0; i<ld.size(); i++) {
            ld[i] = ld[i] / (LDAY_STOP_HOUR - LDAY_START_HOUR)
            le[i] = le[i] / (LEVE_STOP_HOUR - LEVE_START_HOUR)
            ln[i] = ln[i] / ((24.0D-LNIG_START_HOUR) + LNIG_STOP_HOUR)
        }
        wjSourcesD.add(ld)
        wjSourcesE.add(le)
        wjSourcesN.add(ln)
//        wjSourcesD.add(ComputeRays.dbaToW(res_d))
//        wjSourcesE.add(ComputeRays.dbaToW(res_e))
//        wjSourcesN.add(ComputeRays.dbaToW(res_n))


    }

    @Override
    void addReceiver(long pk, Coordinate position, SpatialResultSet rs) {
        position.z = 4.0
        super.addReceiver(pk, position, rs)
    }

    @Override
    public double[] getMaximalSourcePower(int sourceId) {
        return wjSourcesD.get(sourceId)
    }
}
