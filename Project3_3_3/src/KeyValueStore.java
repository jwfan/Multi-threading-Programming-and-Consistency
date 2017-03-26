import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.PriorityQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
	/* TODO: Add code to implement your backend storage */
	ConcurrentHashMap<String, String> dataCenter = new ConcurrentHashMap<String, String>();
	ConcurrentHashMap<String, PriorityQueue<Long>> putMap = new ConcurrentHashMap<String, PriorityQueue<Long>>();
	ConcurrentHashMap<String, Long> keyTimestampMap = new ConcurrentHashMap<String, Long>();
	// ConcurrentHashMap<String, PriorityBlockingQueue<String>> getMap = new
	// ConcurrentHashMap<String, PriorityBlockingQueue<String>>();

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);
		// Each put operation in to specific datacenter is blocked until all
		// previous put and get is down
		// In eventual consistency, all operation come without order
		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				String value = map.get("value");
				String consistency = map.get("consistency");
				Integer region = Integer.parseInt(map.get("region"));
				String timestamp = map.get("timestamp");
				Long longtimestamp = Long.parseLong(map.get("timestamp"));
				/*
				 * TODO: Add code here to handle the put request Remember to use
				 * the explicit timestamp if needed!
				 */
				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						if (consistency.equals("strong")) {
							synchronized (putMap.get(key)) {
								while (putMap.containsKey(key) && putMap.get(key).peek()<longtimestamp) {
									try {
										putMap.get(key).wait();
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
								dataCenter.put(key, value);
								String response = "stored";
								req.response().putHeader("Content-Type", "text/plain");
								req.response().putHeader("Content-Length", String.valueOf(response.length()));
								req.response().end(response);
								req.response().close();
							}
						} else {
							if(!keyTimestampMap.containsKey(key)){
								keyTimestampMap.put(key, longtimestamp);
							}
							synchronized (keyTimestampMap.get(key)) {
								if (keyTimestampMap.get(key)<=longtimestamp) {
									dataCenter.put(key, value);
									keyTimestampMap.put(key, longtimestamp);String response = "stored";
									req.response().putHeader("Content-Type", "text/plain");
									req.response().putHeader("Content-Length", String.valueOf(response.length()));
									req.response().end(response);
									req.response().close();
								}
								else{
									String response = "stored";
									req.response().putHeader("Content-Type", "text/plain");
									req.response().putHeader("Content-Length", String.valueOf(response.length()));
									req.response().end(response);
									req.response().close();
								}
							}
						}

					}
				});
				t.start();
			}
		});
		// Each get operation in datacenter keep tracking the putmap until all
		// previous put are down. Here a sleep(0) is used to release system
		// occupation.
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				String consistency = map.get("consistency");
				String timestamp = map.get("timestamp");
				Long longtimestamp = Long.parseLong(map.get("timestamp"));
				/*
				 * TODO: Add code here to handle the get request Remember that
				 * you may need to do some locking for this
				 */
				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						if (consistency.equals("strong")) {
							while (putMap.containsKey(key) && !putMap.get(key).isEmpty()
									&& putMap.get(key).peek() != null
									&& putMap.get(key).peek()<longtimestamp) {
								try {
									Thread.sleep(0);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							String response = "";
							if (dataCenter.containsKey(key))
								response = dataCenter.get(key);
							req.response().putHeader("Content-Type", "text/plain");
							if (response == null || response.equals("")) {
								response = "0";
							}
							if (response != null)
								req.response().putHeader("Content-Length", String.valueOf(response.length()));
							req.response().end(response);
							req.response().close();
						} else {
							String response = "";
							if (dataCenter.containsKey(key))
								response = dataCenter.get(key);
							req.response().putHeader("Content-Type", "text/plain");
							if (response == null || response.equals("")) {
								response = "0";
							}
							if (response != null)
								req.response().putHeader("Content-Length", String.valueOf(response.length()));
							req.response().end(response);
							req.response().close();
						}
					}
				});
				t.start();
			}
		});
		// Clears stored keys.
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				/*
				 * TODO: Add code to here to flush your datastore. This is
				 * MANDATORY
				 */
				dataCenter.clear();
				putMap.clear();
				keyTimestampMap.clear();
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the AHEAD is called
		// Each ahead create a thread and synchronize the change of put map
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				String timestamp = map.get("timestamp");

				Long longtimestamp = Long.parseLong(map.get("timestamp"));
				/* TODO: Add code to handle the signal here if you wish */
				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						synchronized (this) {

							if (!putMap.containsKey(key)) {
								putMap.put(key, new PriorityQueue<Long>(11, new Comparator<Long>() {

									@Override
									public int compare(Long o1, Long o2) {
										// TODO Auto-generated method stub
										return o1-o2>0?1:(o1-o2==0?0:-1);
									}
								
								}));
							}
							putMap.get(key).add(longtimestamp);
						}
					}
				});
				t.start();
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the COMPLETE is called
		// Each complete is called when a put to all datacenter operation
		// finished and remove this put from putmap, then notify waiting put
		// thread
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				String timestamp = map.get("timestamp");

				Long longtimestamp = Long.parseLong(map.get("timestamp"));
				/* TODO: Add code to handle the signal here if you wish */
				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						synchronized (putMap.get(key)) {
							putMap.get(key).remove(longtimestamp);
							putMap.get(key).notifyAll();
						}
					}
				});
				t.start();
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
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
