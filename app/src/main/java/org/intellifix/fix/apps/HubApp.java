package org.intellifix.fix.apps;

import lombok.extern.slf4j.Slf4j;
import org.intellifix.redis.base.MessagePublisher;
import quickfix.*;
import quickfix.field.MsgType;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HubApp extends MessageCracker implements Application {
    private volatile SessionID clientSession;
    private volatile SessionID brokerSession;

    private MessagePublisher messagePublisher;
    private final String clientCompID;
    private final String brokerCompID;
    private final Random random = new Random();
    private final Map<String, String> clOrdIDMap = new ConcurrentHashMap<>();

    public HubApp(MessagePublisher messagePublisher, String clientCompID, String brokerCompID) {
        this.messagePublisher = messagePublisher;
        this.clientCompID = clientCompID;
        this.brokerCompID = brokerCompID;
    }

    @Override
    public void onCreate(SessionID sessionID) {
        System.out.println("[HUB] onCreate: " + sessionID);
    }

    @Override
    public void onLogon(SessionID sessionID) {
        log.info("[HUB] onLogon: " + sessionID);
        if (clientCompID.equals(sessionID.getTargetCompID())) {
            clientSession = sessionID;
        } else if (brokerCompID.equals(sessionID.getTargetCompID())) {
            brokerSession = sessionID;
        }
    }

    @Override
    public void onLogout(SessionID sessionID) {
        log.info("[HUB] onLogout: " + sessionID);
        if (sessionID.equals(clientSession)) {
            clientSession = null;
        } else if (sessionID.equals(brokerSession)) {
            brokerSession = null;
        }
    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    }

    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {

    }

    @Override
    public void fromApp(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {

        String msgType = message.getHeader().getString(MsgType.FIELD);
        String sender = sessionID.getSenderCompID();
        String target = sessionID.getTargetCompID();

        log.info("[HUB] Received 35=" + msgType + " from " + sessionID.getTargetCompID());

        if (sessionID.equals(clientSession)) {
            if (brokerSession != null) {
                log.info("[HUB] Forwarding to Broker...session:" + brokerSession);
                String tag11 = message.getString(11);
                message.setString(526, tag11);
                String newTag11 = getRunningNumber();
                clOrdIDMap.put(newTag11, tag11);
                message.setString(11, newTag11);
                forward(message, brokerSession);
            } else {
                log.info("[HUB] WARN: Broker session not logged on. Cannot forward.");
            }
        } else if (sessionID.equals(brokerSession)) {
            if (clientSession != null) {
                log.info("[HUB] Forwarding to Client...session: " + clientSession);
                if (message.isSetField(11)) {
                    String simulatedClOrdID = clOrdIDMap.get(message.getString(11));
                    message.setString(11, simulatedClOrdID);
                    log.info("[HUB] Restored tag 11 to " + simulatedClOrdID);
                }
                forward(message, clientSession);
            } else {
                log.info("[HUB] WARN: Client session not logged on. Cannot forward.");
            }
        }
    }

    private void forward(Message message, SessionID targetSessionID) {
        try {
            String rawMessage = message.toString();
            Message forwardMsg = new Message();
            forwardMsg.fromString(rawMessage, null, false);
            Session.sendToTarget(forwardMsg, targetSessionID);
        } catch (Exception e) {
            log.error("[HUB] Error forwarding message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String pretty(Message m) {
        return m.toString().replace('\u0001', '|');
    }

    private String getRunningNumber() {
        return "%d%d".formatted(random.nextInt(1000, 10000), System.currentTimeMillis());
    }
}
