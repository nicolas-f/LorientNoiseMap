package org.noise_planet.roademission

import groovy.transform.CompileStatic
import org.locationtech.jts.geom.Coordinate
import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.GeoJSONDocument
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.KMLDocument
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

@CompileStatic
class PropagationPathStorageFactory implements PointNoiseMap.IComputeRaysOutFactory {
    ConcurrentLinkedDeque<PointToPointPaths> pathQueue = new ConcurrentLinkedDeque<>()
    GZIPOutputStream gzipOutputStream
    AtomicBoolean waitForMorePaths = new AtomicBoolean(true)
    public static final int GZIP_CACHE_SIZE = (int)Math.pow(2, 19)
    String workingDir

    void openPathOutputFile(String path) {
        gzipOutputStream = new GZIPOutputStream(new FileOutputStream(path), GZIP_CACHE_SIZE)
        new Thread(new WriteThread(pathQueue, waitForMorePaths, gzipOutputStream)).start()
    }

    void setWorkingDir(String workingDir) {
        this.workingDir = workingDir
    }

    void exportDomain(PropagationProcessData inputData, String path) {
        GeoJSONDocument geoJSONDocument = new GeoJSONDocument(new FileOutputStream(path))
        geoJSONDocument.writeHeader()
        geoJSONDocument.writeTopographic(inputData.freeFieldFinder.getTriangles(), inputData.freeFieldFinder.getVertices())
        geoJSONDocument.writeFooter()
        KMLDocument kmlDocument

        ZipOutputStream compressedDoc
        System.println( "Cellid" + inputData.cellId.toString())
        compressedDoc = new ZipOutputStream(new FileOutputStream(
                String.format("domain_%d.kmz", inputData.cellId)))
        compressedDoc.putNextEntry(new ZipEntry("doc.kml"))
        kmlDocument = new KMLDocument(compressedDoc)
        kmlDocument.writeHeader()
        kmlDocument.setInputCRS("EPSG:2154")
        kmlDocument.setOffset(new Coordinate(0,0,0))
        kmlDocument.writeTopographic(inputData.freeFieldFinder.getTriangles(), inputData.freeFieldFinder.getVertices())
        kmlDocument.writeBuildings(inputData.freeFieldFinder)
        kmlDocument.writeFooter()
        compressedDoc.closeEntry()
        compressedDoc.close()
    }

    @Override
    IComputeRaysOut create(PropagationProcessData propagationProcessData, PropagationProcessPathData propagationProcessPathData) {
        exportDomain(propagationProcessData, new File(this.workingDir, String.format("_%d.geojson", propagationProcessData.cellId)).absolutePath)
        return new PropagationPathStorage(propagationProcessData, propagationProcessPathData, pathQueue)
    }

    void closeWriteThread() {
        waitForMorePaths.set(false)
    }

    /**
     * Write paths on disk using a single thread
     */
    static class WriteThread implements Runnable {
        ConcurrentLinkedDeque<PointToPointPaths> pathQueue
        AtomicBoolean waitForMorePaths
        GZIPOutputStream gzipOutputStream

        WriteThread(ConcurrentLinkedDeque<PointToPointPaths> pathQueue, AtomicBoolean waitForMorePaths, GZIPOutputStream gzipOutputStream) {
            this.pathQueue = pathQueue
            this.waitForMorePaths = waitForMorePaths
            this.gzipOutputStream = gzipOutputStream
        }

        @Override
        void run() {
            long exportReceiverRay = 1 // primary key of receiver to export
            KMLDocument kmlDocument

            ZipOutputStream compressedDoc

            compressedDoc = new ZipOutputStream(new FileOutputStream(
                    String.format("domain.kmz")))
            compressedDoc.putNextEntry(new ZipEntry("doc.kml"))
            kmlDocument = new KMLDocument(compressedDoc)
            kmlDocument.writeHeader()
            kmlDocument.setInputCRS("EPSG:2154")
            kmlDocument.setOffset(new Coordinate(0,0,0))


            /*PropagationProcessPathData genericMeteoData = new PropagationProcessPathData()
            genericMeteoData.setHumidity(70)
            genericMeteoData.setTemperature(10)
            ComputeRaysOut out = new ComputeRaysOut(false, genericMeteoData)*/

            DataOutputStream dataOutputStream = new DataOutputStream(gzipOutputStream)
            while (waitForMorePaths.get()) {
                while(!pathQueue.isEmpty()) {
                    PointToPointPaths paths = pathQueue.pop()
                    paths.writePropagationPathListStream(dataOutputStream)

                    if(paths.receiverId == exportReceiverRay) {
                        // Export rays
                        kmlDocument.writeRays(paths.getPropagationPathList())
                       // out.computeAttenuation(genericMeteoData, paths.sourceId, 1.0, paths.receiverId, paths.getPropagationPathList())


                    }

                }
                Thread.sleep(10)
            }
            dataOutputStream.flush()
            gzipOutputStream.close()
            kmlDocument.writeFooter()
            compressedDoc.closeEntry()
            compressedDoc.close()



        }
    }
}
