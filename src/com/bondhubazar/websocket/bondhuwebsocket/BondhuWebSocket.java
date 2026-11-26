
package com.bondhubazar.websocket.bondhuwebsocket;

// Android Handler: UI Thread-এ Event পাঠানোর জন্য
import android.os.Handler;
import android.os.Looper;

// Java URL Encoding
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

// Java concurrency
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// JSON
import org.json.JSONObject;

// OkHttp WebSocket
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

// App Inventor
import com.google.appinventor.components.annotations.Extension;
import com.google.appinventor.components.annotations.SimpleFunction;
import com.google.appinventor.components.annotations.SimpleEvent;

import com.google.appinventor.components.runtime.AndroidNonvisibleComponent;
import com.google.appinventor.components.runtime.ComponentContainer;
import com.google.appinventor.components.runtime.EventDispatcher;

@Extension(
    description = "Bondhu Bazar Supabase Realtime WebSocket"
)

public class BondhuWebSocket
    extends AndroidNonvisibleComponent {

    // Android UI Thread Handler
    private final Handler mainHandler =
        new Handler(Looper.getMainLooper());

    // WebSocket Client
    private OkHttpClient client;

    // Active WebSocket
    private WebSocket webSocket;

    // Scheduled tasks
    private ScheduledExecutorService scheduler;

    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> reconnectTask;

    // Connection information
    private String projectUrl = "";
    private String publishableKey = "";
    private String accessToken = "";
    private String channelName = "";

    // Channel Join Reference
    private String joinRef = "1";

    // Message reference counter
    private AtomicInteger refCounter = new AtomicInteger(1);

    // Connection state
    private volatile boolean connected = false;
    private volatile boolean joined = false;
    private volatile boolean shouldReconnect = false;

    // Reconnect counter
    private int reconnectAttempts = 0;

    // Constructor
    public BondhuWebSocket(ComponentContainer container) {

        super(container.$form());

        client = new OkHttpClient.Builder()
            .pingInterval(20, TimeUnit.SECONDS)
            .build();

        scheduler =
            Executors.newSingleThreadScheduledExecutor();
    }

    // =========================================
    // ১. Supabase WebSocket Connect
    // =========================================

    @SimpleFunction(
        description = "Connect to Supabase Realtime private channel"
    )
    public void Connect(
        String url,
        String key,
        String token,
        String topic
    ) {

        if (url == null || key == null ||
            token == null || topic == null ||
            url.trim().isEmpty() ||
            key.trim().isEmpty() ||
            token.trim().isEmpty() ||
            topic.trim().isEmpty()) {

            PostError("Project URL, key, token and topic are required");
            return;
        }

        // আগের সংযোগ বন্ধ
        Disconnect();

        projectUrl = url.trim();
        publishableKey = key.trim();
        accessToken = token.trim();
        channelName = topic.trim();

        shouldReconnect = true;
        reconnectAttempts = 0;

        // নতুন Scheduler
        scheduler =
            Executors.newSingleThreadScheduledExecutor();

        OpenSocket();
    }

    // =========================================
    // ২. WebSocket খোলা
    // =========================================

    private void OpenSocket() {

        try {

            String baseUrl = projectUrl
                .replaceFirst("^https://", "wss://")
                .replaceFirst("^http://", "ws://");

            if (!baseUrl.startsWith("wss://")) {
                PostError("Invalid Supabase Project URL");
                return;
            }

            if (baseUrl.endsWith("/")) {
                baseUrl = baseUrl.substring(
                    0,
                    baseUrl.length() - 1
                );
            }

            String socketUrl =
                baseUrl +
                "/realtime/v1/websocket?apikey=" +
                URLEncoder.encode(
                    publishableKey,
                    "UTF-8"
                ) +
                "&vsn=1.0.0";

            Request request = new Request.Builder()
                .url(socketUrl)
                .build();

            webSocket = client.newWebSocket(
                request,
                new WebSocketListener() {

                    @Override
                    public void onOpen(
                        WebSocket socket,
                        Response response
                    ) {

                        connected = true;
                        joined = false;

                        reconnectAttempts = 0;

                        PostConnectionStatus("CONNECTED");

                        StartHeartbeat();

                        JoinChannel();
                    }

                    @Override
                    public void onMessage(
                        WebSocket socket,
                        String text
                    ) {

                        HandleMessage(text);
                    }

                    @Override
                    public void onClosing(
                        WebSocket socket,
                        int code,
                        String reason
                    ) {

                        socket.close(code, reason);
                    }

                    @Override
                    public void onClosed(
                        WebSocket socket,
                        int code,
                        String reason
                    ) {

                        connected = false;
                        joined = false;

                        StopHeartbeat();

                        PostConnectionStatus("DISCONNECTED");

                        ScheduleReconnect();
                    }

                    @Override
                    public void onFailure(
                        WebSocket socket,
                        Throwable error,
                        Response response
                    ) {

                        connected = false;
                        joined = false;

                        StopHeartbeat();

                        PostError(
                            error.getMessage() == null
                                ? "WebSocket connection failed"
                                : error.getMessage()
                        );

                        PostConnectionStatus("ERROR");

                        ScheduleReconnect();
                    }
                }
            );

        } catch (Exception e) {

            PostError(e.getMessage());
            ScheduleReconnect();
        }
    }

    // =========================================
    // ৩. Private Channel Join
    // =========================================

    private void JoinChannel() {

        try {

            JSONObject config = new JSONObject();

            JSONObject broadcast = new JSONObject();

            broadcast.put("ack", true);
            broadcast.put("self", false);

            JSONObject presence = new JSONObject();

            presence.put("enabled", false);

            config.put("broadcast", broadcast);
            config.put("presence", presence);

            // Private Channel
            config.put("private", true);

            JSONObject payload = new JSONObject();

            payload.put("config", config);

            // User JWT
            payload.put("access_token", accessToken);

            JSONObject message = new JSONObject();

            message.put("topic", GetTopic());
            message.put("event", "phx_join");
            message.put("payload", payload);
            message.put("ref", joinRef);
            message.put("join_ref", joinRef);

            SendRaw(message);

        } catch (Exception e) {

            PostError(e.getMessage());
        }
    }

    // =========================================
    // ৪. Topic তৈরি
    // =========================================

    private String GetTopic() {

        return channelName.startsWith("realtime:")
            ? channelName
            : "realtime:" + channelName;
    }

    // =========================================
    // ৫. Message গ্রহণ
    // =========================================

    private void HandleMessage(String text) {

        try {

            JSONObject message = new JSONObject(text);

            String event = message.optString("event", "");

            JSONObject payload =
                message.optJSONObject("payload");

            // Channel Join সফল হয়েছে কি না
            if ("phx_reply".equals(event)) {

                if (payload != null &&
                    "ok".equals(
                        payload.optString("status")
                    )) {

                    joined = true;

                    PostConnectionStatus("JOINED");
                    ChannelJoined(channelName);
                }

                return;
            }

            // Broadcast Message
            if ("broadcast".equals(event)) {

                if (payload == null) {
                    return;
                }

                String customEvent =
                    payload.optString("event", "");

                JSONObject userPayload =
                    payload.optJSONObject("payload");

                String data = userPayload == null
                    ? payload.optString("payload", "")
                    : userPayload.toString();

                PostMessage(
                    customEvent,
                    data
                );

                return;
            }

            // Server Error
            if ("phx_error".equals(event) ||
                "phx_close".equals(event)) {

                joined = false;

                PostConnectionStatus("CHANNEL_CLOSED");

                return;
            }

        } catch (Exception e) {

            PostError("Message parsing failed: " +
                e.getMessage());
        }
    }

    // =========================================
    // ৬. Broadcast Message পাঠানো
    // =========================================

    @SimpleFunction(
        description = "Send JSON broadcast message"
    )
    public boolean SendBroadcast(
        String eventName,
        String payloadJson
    ) {

        if (!connected || !joined || webSocket == null) {

            PostError("WebSocket channel is not joined");

            return false;
        }

        try {

            JSONObject userPayload =
                new JSONObject(payloadJson);

            JSONObject payload = new JSONObject();

            payload.put("event", eventName);
            payload.put("payload", userPayload);

            JSONObject message = new JSONObject();

            String ref = String.valueOf(
                refCounter.incrementAndGet()
            );

            message.put("topic", GetTopic());
            message.put("event", "broadcast");
            message.put("payload", payload);
            message.put("ref", ref);
            message.put("join_ref", joinRef);

            return SendRaw(message);

        } catch (Exception e) {

            PostError("Invalid JSON: " + e.getMessage());

            return false;
        }
    }

    // =========================================
    // ৭. WebSocket Message পাঠানো
    // =========================================

    private boolean SendRaw(JSONObject message) {

        if (webSocket == null || !connected) {
            return false;
        }

        return webSocket.send(message.toString());
    }

    // =========================================
    // ৮. Heartbeat চালু
    // =========================================

    private void StartHeartbeat() {

        StopHeartbeat();

        heartbeatTask = scheduler.scheduleAtFixedRate(
            new Runnable() {

                @Override
                public void run() {

                    if (!connected) {
                        return;
                    }

                    try {

                        JSONObject message =
                            new JSONObject();

                        message.put("topic", "phoenix");
                        message.put("event", "heartbeat");
                        message.put("payload", new JSONObject());

                        message.put(
                            "ref",
                            String.valueOf(
                                refCounter.incrementAndGet()
                            )
                        );

                        message.put("join_ref", JSONObject.NULL);

                        SendRaw(message);

                    } catch (Exception e) {

                        PostError(e.getMessage());
                    }
                }

            },
            25,
            25,
            TimeUnit.SECONDS
        );
    }

    // =========================================
    // ৯. Heartbeat বন্ধ
    // =========================================

    private void StopHeartbeat() {

        if (heartbeatTask != null) {

            heartbeatTask.cancel(true);

            heartbeatTask = null;
        }
    }

    // =========================================
    // ১০. Automatic Reconnect
    // =========================================

    private synchronized void ScheduleReconnect() {

        if (!shouldReconnect ||
            scheduler == null ||
            scheduler.isShutdown() ||
            reconnectTask != null) {

            return;
        }

        int[] delays = {2, 5, 10, 20, 30};

        int index = Math.min(
            reconnectAttempts,
            delays.length - 1
        );

        int delay = delays[index];

        reconnectAttempts++;

        PostConnectionStatus("RECONNECTING");

        reconnectTask = scheduler.schedule(
            new Runnable() {

                @Override
                public void run() {

                    reconnectTask = null;

                    if (shouldReconnect && !connected) {
                        OpenSocket();
                    }
                }

            },
            delay,
            TimeUnit.SECONDS
        );
    }

    // =========================================
    // ১১. Disconnect
    // =========================================

    @SimpleFunction(
        description = "Disconnect from Supabase Realtime"
    )
    public void Disconnect() {

        shouldReconnect = false;

        StopHeartbeat();

        if (reconnectTask != null) {

            reconnectTask.cancel(true);

            reconnectTask = null;
        }

        if (webSocket != null) {

            webSocket.close(1000, "User disconnected");

            webSocket = null;
        }

        connected = false;
        joined = false;

        if (scheduler != null) {

            scheduler.shutdownNow();

            scheduler = null;
        }

        PostConnectionStatus("DISCONNECTED");
    }

    // =========================================
    // ১২. Connection পরীক্ষা
    // =========================================

    @SimpleFunction(
        description = "Check WebSocket connection"
    )
    public boolean IsConnected() {

        return connected;
    }

    // =========================================
    // ১৩. Channel Join পরীক্ষা
    // =========================================

    @SimpleFunction(
        description = "Check whether channel is joined"
    )
    public boolean IsJoined() {

        return joined;
    }

    // =========================================
    // ১৪. Event পাঠানোর Helper
    // =========================================

    private void PostMessage(
        final String event,
        final String data
    ) {

        mainHandler.post(new Runnable() {

            @Override
            public void run() {

                MessageReceived(event, data);
            }
        });
    }

    private void PostError(final String error) {

        mainHandler.post(new Runnable() {

            @Override
            public void run() {

                ErrorOccurred(error);
            }
        });
    }

    private void PostConnectionStatus(
        final String status
    ) {

        mainHandler.post(new Runnable() {

            @Override
            public void run() {

                ConnectionStatusChanged(status);
            }
        });
    }

    // =========================================
    // ১৫. Message Received Event
    // =========================================

    @SimpleEvent(
        description = "Triggered when a broadcast message arrives"
    )
    public void MessageReceived(
        String event,
        String data
    ) {

        EventDispatcher.dispatchEvent(
            this,
            "MessageReceived",
            event,
            data
        );
    }

    // =========================================
    // ১৬. Connection Status Event
    // =========================================

    @SimpleEvent(
        description = "Triggered when connection status changes"
    )
    public void ConnectionStatusChanged(String status) {

        EventDispatcher.dispatchEvent(
            this,
            "ConnectionStatusChanged",
            status
        );
    }

    // =========================================
    // ১৭. Channel Joined Event
    // =========================================

    @SimpleEvent(
        description = "Triggered when channel is joined"
    )
    public void ChannelJoined(String topic) {

        EventDispatcher.dispatchEvent(
            this,
            "ChannelJoined",
            topic
        );
    }

    // =========================================
    // ১৮. Error Event
    // =========================================

    @SimpleEvent(
        description = "Triggered when an error occurs"
    )
    public void ErrorOccurred(String message) {

        EventDispatcher.dispatchEvent(
            this,
            "ErrorOccurred",
            message
        );
    }
}
