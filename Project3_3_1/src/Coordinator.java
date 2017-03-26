import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.net.URL;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class Coordinator extends Verticle {

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-54-224-228-217.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-54-174-198-84.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-54-227-116-238.compute-1.amazonaws.com";
	// Set thread-free data type to record put and key status
	AtomicBoolean isGet = new AtomicBoolean(false);
	AtomicBoolean isPut = new AtomicBoolean(false);
	Object lockKey = new Object();
	ConcurrentHashMap<String, PriorityBlockingQueue<String>> keyMap = new ConcurrentHashMap<String, PriorityBlockingQueue<String>>();
	ConcurrentHashMap<String, AtomicBoolean> putMap = new ConcurrentHashMap<String, AtomicBoolean>();

	/*
	 * public void updateCurrent(String key) { synchronized (key) {
	 * currentThread = keyMap.get(key).poll(); } }
	 */
	@Override
	public void start() {
		// DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
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
				// You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(
						System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset()).toString();
				// Sort by timestamp useing PriorityBlockingQueue
				if (!keyMap.containsKey(key)) {
					keyMap.put(key, new PriorityBlockingQueue<String>(11, new Comparator<String>() {
						@Override
						public int compare(String o1, String o2) {
							// TODO Auto-generated method stub
							return o1.compareTo(o2);
						}
					}));
				}
				keyMap.get(key).add(timestamp);

				Thread t = new Thread(new Runnable() {
					public void run() {
						// TODO: Write code for PUT operation here.
						// Each PUT operation is handled in a different thread.
						// Highly recommended that you make use of helper
						// functions.

						// Only execute first thread in keyMap, and get status
						// should be false
						// After waked and operation done, modify status and
						// notify all thread
						synchronized (keyMap.get(key)) {
							while (!timestamp.equals(keyMap.get(key).peek()) || isGet.get()) {
								try {
									keyMap.get(key).wait();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							putMap.put(key, new AtomicBoolean(true));
							try {
								KeyValueLib.PUT(dataCenter1, key, value);
								KeyValueLib.PUT(dataCenter2, key, value);
								KeyValueLib.PUT(dataCenter3, key, value);
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
				// Every important notice should be repeated for three times
				// Do not remove this
				// Do not remove this
				// Do not remove this
				req.response().end();

			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				// You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(
						System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset()).toString();
				Thread t = new Thread(new Runnable() {
					public void run() {
						// TODO: Write code for GET operation here.
						// Each GET operation is handled in a different thread.
						// Highly recommended that you make use of helper
						// functions.
						if (!keyMap.containsKey(key)) {
							keyMap.put(key, new PriorityBlockingQueue<String>(11, new Comparator<String>() {
								@Override
								public int compare(String o1, String o2) {
									// TODO Auto-generated method stub
									return o2.compareTo(o1);
								}
							}));
						}
						// Get only be stuck when Put status is true
						// When all status are false, decide next operation
						// based on timestamp
						// Modify status and notify all thread after get done
						synchronized (keyMap.get(key)) {
							while ((putMap.containsKey(key) && putMap.get(key).get() == true)
									|| (keyMap.containsKey(key) && !keyMap.get(key).isEmpty()
											&& timestamp.compareTo(keyMap.get(key).peek()) >= 0)) {
								try {
									keyMap.get(key).wait();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							isGet.set(true);
							String result = "0";
							try {
								result = loc.equals("1") ? KeyValueLib.GET(dataCenter1, key)
										: (loc.equals("2") ? KeyValueLib.GET(dataCenter2, key)
												: (loc.equals("3") ? KeyValueLib.GET(dataCenter3, key) : "0"));
								if (result.equals("null")) {
									result = "0";
								}
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							isGet.set(false);
							keyMap.get(key).notifyAll();
							// req.response().end("0");
							req.response().end(result);
						}
					}
				});
				t.start();
			}
		});

		routeMatcher.get("/flush", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				// Flush all datacenters before each test.
				URL url = null;
				try {
					flush(dataCenter1);
					flush(dataCenter2);
					flush(dataCenter3);
				} catch (Exception e) {
					e.printStackTrace();
				}
				// This endpoint will be used by the auto-grader to flush your
				// datacenter before tests
				// You can initialize/re-initialize the required data structures
				// here
				req.response().end();
			}

			private void flush(String dataCenter) throws Exception {
				URL url = new URL("http://" + dataCenter + ":8080/flush");
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openConnection().getInputStream()));
				String inputLine;
				while ((inputLine = in.readLine()) != null)
					;
				in.close();
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
