import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.LinkedList;
import java.util.Map;

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
	ConcurrentHashMap<String, PriorityBlockingQueue<String>> keyMap = new ConcurrentHashMap<String, PriorityBlockingQueue<String>>();
	ConcurrentHashMap<String, AtomicBoolean> putMap = new ConcurrentHashMap<String, AtomicBoolean>();

	/*
	 * This is a hash function to map each key to one Coordinator
	 */
	public String hashMapKey(String key) {
		switch (key) {
		case "a":
			return "dataCenterUSE,1";
		case "b":
			return "dataCenterUSW,2";
		case "c":
			return "coordinatorSING,3";
		default:
			return "";
		}
	}

	/*
	 * This function is used to switch waiting time when put operation is sent
	 * by either client or other coordinator to the correct Primary Coordinator.
	 */
	public Integer waitTime(String hashMapKey, String forwardedRegion) {
		if (forwardedRegion.isEmpty()) {
			switch (hashMapKey) {
			case "dataCenterUSE,1":
				return 600;
			case "dataCenterUSW,2":
				return 800;
			case "coordinatorSING,3":
				return 800;
			default:
				return 0;
			}
		} else {
			switch (hashMapKey) {
			case "dataCenterUSE,1":
				switch (forwardedRegion) {
				case "dataCenterUSW":
					return 200;
				default:
					return 0;
				}
			case "dataCenterUSW,2":
				switch (forwardedRegion) {
				case "dataCenterUSE":
					return 600;
				default:
					return 0;
				}
			case "coordinatorSING,3":
				switch (forwardedRegion) {
				case "dataCenterUSE":
					return 200;
				default:
					return 0;
				}
			default:
				return 0;
			}
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
				String forwardRegion = hashMapKey(key);
				// hash map
				if (!forwardRegion.substring(0, forwardRegion.indexOf(",")).equals(forwardedRegion)) {
					try {
						KeyValueLib.FORWARD(forwardRegion.substring(0, forwardRegion.indexOf(",")), key, value, timestamp);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					if (!keyMap.containsKey(key)) {
						keyMap.put(key, new PriorityBlockingQueue<String>(11, new Comparator<String>() {
							@Override
							public int compare(String o1, String o2) {
								// TODO Auto-generated method stub
								return o1.compareTo(o2);
							}
						}));
					}
					Thread t = new Thread(new Runnable() {
						public void run() {
							/*
							 * TODO: Add code for PUT request handling here Each
							 * operation is handled in a new thread. Use of
							 * helper functions is highly recommended
							 */
							try {
								Thread.sleep(waitTime(forwardRegion, forwardedRegion));
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							keyMap.get(key).add(timestamp);
							synchronized (keyMap.get(key)) {
								while (!timestamp.equals(keyMap.get(key).peek())) {
									try {
										keyMap.get(key).wait();
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
								try {
									KeyValueLib.AHEAD(key, timestamp);
								} catch (IOException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
								putMap.put(key, new AtomicBoolean(true));
								try {
									KeyValueLib.PUT(dataCenterUSE, key, value, timestamp, consistencyType);
									KeyValueLib.PUT(dataCenterUSW, key, value, timestamp, consistencyType);
									KeyValueLib.PUT(dataCenterSING, key, value, timestamp, consistencyType);
									KeyValueLib.COMPLETE(key, timestamp);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								keyMap.get(key).remove(timestamp);
								putMap.put(key, new AtomicBoolean(false));
								keyMap.get(key).notifyAll();
							}

						}
					});
					t.start();
					req.response().end(); // Do not remove this
				}
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Long longtimestamp = Long.parseLong(map.get("timestamp"));
				final String timestamp = map.get("timestamp");
				String forwardRegion = hashMapKey(key);
				Thread t = new Thread(new Runnable() {
					public void run() {
						/*
						 * TODO: Add code for GET requests handling here Each
						 * operation is handled in a new thread. Use of helper
						 * functions is highly recommended
						 */
						String response = "0";
						try {
							response = KeyValueLib.GET(forwardRegion.substring(0, forwardRegion.indexOf(",")), key,
									timestamp, "strong");
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
