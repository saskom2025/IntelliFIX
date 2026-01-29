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
    }

    @Override
    public void fromApp(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {

        String msgType = message.getHeader().getString(MsgType.FIELD);
        messagePublisher.publishMessage("[HUB->CLIENT] Forwarding 35=" + msgType + " " + pretty(message));

        Message expected = this.expectedInbound;
        CountDownLatch latch = this.expectedLatch;

        if (expected != null && latch != null) {
            if (matchesExpected(expected, message)) {
                log.info("[MATCH] Expected inbound satisfied.");
                latch.countDown();
            } else {
                // Not a match; ignore and keep waiting
            }
        }

        // crack(message, sessionID);
    }

    public void onMessage(ExecutionReport report, SessionID sessionID) {
    }

    /**
     * Matching strategy:
     * - Must match MsgType
     * - If expected contains tags, verify those tags match in actual (common: 11,
     * 41, 150, 39, 37, 17)
     */
    private boolean matchesExpected(Message expected, Message actual) {
        try {
            String expType = expected.getHeader().getString(MsgType.FIELD);
            String actType = actual.getHeader().getString(MsgType.FIELD);
            if (!expType.equals(actType))
                return false;

            int[] mandatoryTags = new int[] {
                    11, // ClOrdID
                    41, // OrigClOrdID
                    150, // ExecType
                    39, // OrdStatus
                    37, // OrderID
                    17 // ExecID
            };

            for (int tag : mandatoryTags) {
                if (expected.isSetField(tag)) {
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
