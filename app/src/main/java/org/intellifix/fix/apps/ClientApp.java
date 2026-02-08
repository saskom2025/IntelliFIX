package org.intellifix.fix.apps;

import lombok.extern.slf4j.Slf4j;
import org.intellifix.fix.base.SimulatorAppBase;
import org.intellifix.redis.base.MessagePublisher;
import quickfix.*;
import quickfix.field.MsgType;
import quickfix.fix44.ExecutionReport;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ClientApp extends MessageCracker implements Application, SimulatorAppBase {

    private volatile SessionID activeSession;
    private volatile boolean loggedOn = false;
    private volatile Message expectedInbound = null;
    private volatile CountDownLatch expectedLatch = null;
    private MessagePublisher messagePublisher;

    public ClientApp(SessionSettings settings, DataDictionary dd, MessagePublisher messagePublisher) {
        this.messagePublisher = messagePublisher;
    }

    public void awaitLogon(long timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (!loggedOn && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        if (!loggedOn)
            throw new RuntimeException("Logon not completed within timeout");
    }

    public void setExpectedInbound(Message expected, CountDownLatch latch) {
        this.expectedInbound = expected;
        this.expectedLatch = latch;
    }

    public void clearExpectedInbound() {
        this.expectedInbound = null;
        this.expectedLatch = null;
    }

    public boolean isLoggedOn() {
        return loggedOn;
    }

    public SessionID getActiveSession() {
        return activeSession;
    }

    @Override
    public Message updateTagEleven(Message message, String simId, String senderCompId, String tag) {
        var updatedTagEleven = "%s-%s-%s".formatted(simId, senderCompId, tag);
        message.setString(11, updatedTagEleven);
        return message;
    }

    @Override
    public void onCreate(SessionID sessionID) {
        log.info("[CLIENT] onCreate: " + sessionID);
        this.activeSession = sessionID;
    }

    @Override
    public void onLogon(SessionID sessionID) {
        log.info("[CLIENT] onLogon: " + sessionID);
        this.loggedOn = true;
        this.activeSession = sessionID;
    }

    @Override
    public void onLogout(SessionID sessionID) {
        log.info("[CLIENT] onLogout: " + sessionID);
        this.loggedOn = false;
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

        String msgType = null;
        try {
            msgType = message.getHeader().getString(MsgType.FIELD);
        } catch (FieldNotFound e) {
            throw new RuntimeException(e);
        }

        if (msgType.equals(MsgType.ORDER_SINGLE) || msgType.equals(MsgType.ORDER_CANCEL_REPLACE_REQUEST)
                || msgType.equals(MsgType.ORDER_CANCEL_REQUEST)) {
            updatedClientOrderIdForHub(message);
        }

        messagePublisher
                .publishMessage("Client sent 35=" + msgType + " " + pretty(message));
    }

    @Override
    public void fromApp(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {

        String msgType = message.getHeader().getString(MsgType.FIELD);
        messagePublisher.publishMessage("Client received 35=" + msgType + " " + pretty(message));

        Message expected = this.expectedInbound;
        CountDownLatch latch = this.expectedLatch;

        if (expected != null && latch != null) {
            if (matchesExpected(expected, message)) {
                log.info("[MATCH] Expected inbound satisfied.");
                latch.countDown();
            } else {
                System.out.println("[NO_MATCH_FOUND]");
            }
        }
        crack(message, sessionID);
    }

    private void updatedClientOrderIdForHub(Message message) {
        String msgType = null;
        String simId = "sim1";
        try {
            msgType = message.getHeader().getString(MsgType.FIELD);
            String clientOrderID = message.getString(11);
            String senderCompID = message.getHeader().getString(49);

            updateTagEleven(message, simId, senderCompID, clientOrderID);

        } catch (FieldNotFound e) {
            throw new RuntimeException(e);
        }
    }

    public void onMessage(ExecutionReport report, SessionID sessionID) {
    }

    private boolean matchesExpected(Message expected, Message actual) {
        try {
            System.out.println("EXPECTED: "+expected);
            System.out.println("ACTUAL: "+actual);
            String expType = expected.getHeader().getString(MsgType.FIELD);
            String actType = actual.getHeader().getString(MsgType.FIELD);
            if (!expType.equals(actType))
                return false;

            // multiple tags can be added as comma separated values
            int[] mandatoryTags = new int[] {
                    11
            };

            for (int tag : mandatoryTags) {
                if (tag == 11) {
                    // tag 11 validation
                    String expectedVal11 = expected.getString(11);
                    String actualVal11 = null;
                    if (actual.isSetField(11)) {
                        String val11 = actual.getString(11);
                        actualVal11 = val11.contains("-") ? val11.substring(val11.lastIndexOf("-") + 1) : val11;
                    }
                    if (actualVal11 != null) {
                        if (!expectedVal11.equals(actualVal11))
                            return false;
                    }
                } else if (expected.isSetField(tag)) {
                    // other tags validation
                    if (!actual.isSetField(tag))
                        return false;
                    String ev = expected.getString(tag);
                    String av = actual.getString(tag);
                    if (!ev.equals(av))
                        return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String pretty(Message m) {
        return m.toString().replace('\u0001', '|');
    }
}
