package uk.ac.cam.cl.groupproject12.lima.web;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;

import java.io.*;

public class Web {

    private static String webUrl = "http://localhost:8001/java/";

    /**
     * Notifies the web UI that a job has begun.
     *
     * @param ip
     *         The router IP the job is being run upon.
     * @param timestamp
     *         The timestamp of the file the job is running.
     * @param numOfJobs
     *         The number of sub-jobs that run during this job.
     */
    public static void newJob(String ip, long timestamp, int numOfJobs) {
        Web.httpRequest("newJob?ip=" + ip + "&timestamp=" + timestamp
                + "&numOfJobs=" + numOfJobs);
    }

    /**
     * Sends updates of job status to the web UI.
     *
     * @param ip
     *         The router IP the job is being run upon.
     * @param timestamp
     *         The timestamp of the file that the job is running.
     * @param allComplete
     *         A boolean indicator of if all sub-jobs have finished.
     */
    public static void updateJob(String ip, long timestamp,
                                 boolean allComplete) {
        Web.httpRequest("updateJob?ip=" + ip + "&timestamp=" + timestamp
                + "&complete=" + (allComplete ? 1 : 0));
    }

    /**
     * Sends the HTTP request with the given arguments.
     *
     * @param args
     *         Parameters to be added to the end of the query URL.
     */
    private static void httpRequest(String args) {
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(webUrl + args);
        try {
            int statusCode = client.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + method.getStatusLine());
            }
            byte[] responseBody = method.getResponseBody();
            System.out.println(new String(responseBody));
        } catch (HttpException e) {
            System.err.println("Fatal protocol violation: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Fatal transport error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            method.releaseConnection();
        }
    }

}
