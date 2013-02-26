package uk.ac.cam.cl.groupproject12.lima.web;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;

public class Web {

	private static String webUrl = "http://localhost:8000/java/";

	/**
	 * tells the web UI we are starting to run a set of jobs
	 * 
	 * @param ip
	 *            IP of the router status to update
	 * @param status
	 *            0 if router updated, 1 to increment
	 */
	public static void newJob(String ip, String timestamp, int numOfJobs) {
		Web.httpRequest("newJob?ip=" + ip + "&timestamp=" + timestamp
				+ "&numOfJobs=" + numOfJobs);
	}

	/**
	 * send updates of the status of router jobs to the web UI
	 * 
	 * @param ip
	 *            IP of the router status to update
	 * @param allComplete
	 *            true is the entire set of jobs is complete otherwise we inc
	 */
	public static void updateJob(String ip, String timestamp,
			boolean allComplete) {
		Web.httpRequest("updateJob?ip=" + ip + "&timestamp=" + timestamp
				+ "&inc=" + (allComplete ? 1 : 0));
	}

	/**
	 * Used to execute the HTTP request
	 * 
	 * @param args
	 *            Arguments for the HTTP request
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
