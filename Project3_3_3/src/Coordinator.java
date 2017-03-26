import java.io.IOException;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	// This integer variable tells you what region you are in
	// 1 for US-E, 2 for US-W, 3 for Singapore
	private static int region = KeyValueLib.region;

	// Default mode: Strongly consistent
	// Options: strong, eventual
	private static String consistencyType = "strong";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances. Be sure to match the regions with their DNS!
	 * Do the same for the 3 Coordinators as well.
	 */
	private static final String dataCenterUSE = "ec2-54-205-250-50.compute-1.amazonaws.com";
	private static final String dataCenterUSW = "ec2-54-157-223-4.compute-1.amazonaws.com";
	private static final String dataCenterSING = "ec2-34-204-79-205.compute-1.amazonaws.com";

	private static final String coordinatorUSE = "ec2-54-242-180-100.compute-1.amazonaws.com";
	private static final String coordinatorUSW = "ec2-54-145-30-84.compute-1.amazonaws.com";
	private static final String coordinatorSING = "ec2-107-21-81-226.compute-1.amazonaws.com";
	// Get system environment set separately on each coordinator
	private static final String currentCoordinator = System.getenv("LOCALIP");
	// Get system environment set separately on each datacenter
	private static final String currentDataCenter = System.getenv("LOCALDATA");

	/*
	 * This is a hash function to map each key to one Coordinator
	 */
	public String hashMapKey(String key) {
		int v = (key.charAt(0) + 2) % 3;
		switch (v) {
		case 0:
			return coordinatorUSE;
		case 1:
			return coordinatorUSW;
		case 2:
			return coordinatorSING;
		default:
			return coordinatorUSE;
		}
	}

	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenterUSE, 1);
		KeyValueLib.dataCenters.put(dataCenterUSW, 2);
		KeyValueLib.dataCenters.put(dataCenterSING, 3);
		KeyValueLib.coordinators.put(coordinatorUSE, 1);
		KeyValueLib.coordinators.put(coordinatorUSW, 2);
		KeyValueLib.coordinators.put(coordinatorSING, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);
		// Each unforwarded put ahead datastore immediately
		// Then each coordinator send put request immediately to related
		// datacenter or forward to other coordinator according to hashmap
		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				final Long longtimestamp = Long.parseLong(map.get("timestamp"));
				final String timestamp = map.get("timestamp");
				final String forwarded = map.get("forward");
				final String forwardedRegion = map.get("region");
				String forwardRegion = hashMapKey(key);// Primary coordinator
														// determined by hash
														// map
				Thread t = new Thread(new Runnable() {
					public void run() {
						if (consistencyType.equals("strong")&&(forwarded == null || !forwarded.equals("true"))) {
							try {
								KeyValueLib.AHEAD(key, timestamp);
							} catch (IOException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}
						// hash map
						if (forwarded == null
								|| !forwarded.equals("true") && !forwardRegion.equals(currentCoordinator)) {
							try {
								KeyValueLib.FORWARD(forwardRegion, key, value, timestamp);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else {
							/*
							 * TODO: Add code for PUT request handling here Each
							 * operation is handled in a new thread. Use of
							 * helper functions is highly recommended
							 */
							try {
								KeyValueLib.PUT(dataCenterUSE, key, value, timestamp, consistencyType);
								KeyValueLib.PUT(dataCenterUSW, key, value, timestamp, consistencyType);
								KeyValueLib.PUT(dataCenterSING, key, value, timestamp, consistencyType);
								if(consistencyType.equals("strong")){
									KeyValueLib.COMPLETE(key, timestamp);
								}
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}
		});
		// Each coordinator send get operation immediately
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Long longtimestamp = Long.parseLong(map.get("timestamp"));
				final String timestamp = map.get("timestamp");
				Thread t = new Thread(new Runnable() {
					public void run() {
						/*
						 * TODO: Add code for GET requests handling here Each
						 * operation is handled in a new thread. Use of helper
						 * functions is highly recommended
						 */
						String response = "0";
						try {
							response = KeyValueLib.GET(currentDataCenter, key, timestamp, consistencyType);
							if (response.equals("null")) {
								response = "0";
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						req.response().end(response);
					}
				});
				t.start();
			}
		});
		/*
		 * This endpoint is used by the grader to change the consistency level
		 */
		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				consistencyType = map.get("consistency");
				req.response().end();
			}
		});
		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length", String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
}
