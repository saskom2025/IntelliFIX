package org.intellifix.fix.apps;

import org.intellifix.redis.base.MessagePublisher;
import org.intellifix.fix.base.SimulatorAppBase;
import quickfix.*;
import quickfix.field.MsgType;
import quickfix.fix44.ExecutionReport;
import java.util.concurrent.CountDownLatch;

public class BrokerApp extends MessageCracker implements Application, SimulatorAppBase {

    private volatile SessionID activeSession;
    private volatile boolean loggedOn = false;
    private volatile Message expectedInbound = null;
    private volatile CountDownLatch expectedLatch = null;

    private MessagePublisher messagePublisher;

    public BrokerApp(MessagePublisher messagePublisher) {
        this.messagePublisher = messagePublisher;
    }

    public void awaitLogon(long timeoutSeconds) throws InterruptedException {
        var deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (!loggedOn && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        if (!loggedOn)
            throw new RuntimeException("Client did not logon within timeout");
    }

    public SessionID getActiveSession() {
        return activeSession;
    }

    public void setExpectedInbound(Message expected, CountDownLatch latch) {
        this.expectedInbound = expected;
        this.expectedLatch = latch;
    }

    public void clearExpectedInbound() {
        this.expectedInbound = null;
        this.expectedLatch = null;
    }

    @Override
    public void onCreate(SessionID sessionID) {
        System.out.println("[BROKER] onCreate: " + sessionID);
        this.activeSession = sessionID;
    }

    @Override
    public void onLogon(SessionID sessionID) {
        System.out.println("[BROKER] onLogon: " + sessionID);
        this.loggedOn = true;
        this.activeSession = sessionID;
    }

    @Override
    public void onLogout(SessionID sessionID) {
        System.out.println("[BROKER] onLogout: " + sessionID);
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
        messagePublisher.publishMessage("[HUB->BROKER] Forwarding 35=" + msgType + " " + pretty(message));
        // Try to satisfy expected inbound (D/G/F) when waiting
        Message expected = this.expectedInbound;
        CountDownLatch latch = this.expectedLatch;
        if (expected != null && latch != null) {
            if (matchesExpected(expected, message)) {
                System.out.println("[MATCH] Expected inbound satisfied.");
                latch.countDown();
            }
        }

        // crack(message, sessionID);
    }

    public void onMessage(ExecutionReport report, SessionID sessionID) {
    }

    /**
     * Matching strategy for inbound D/G/F:
     * - Must match MsgType
     * - If expected contains certain tags, enforce them
     * (11=ClOrdID, 41=OrigClOrdID, 55, 54)
     */
    private boolean matchesExpected(Message expected, Message actual) {
        try {
            String expType = expected.getHeader().getString(MsgType.FIELD);
            String actType = actual.getHeader().getString(MsgType.FIELD);
            if (!expType.equals(actType))
                return false;

            int[] mandatoryTags = new int[] {
                    11, // ClOrdID
                    41, // OrigClOrdID (for replace/cancel)
                    55, // Symbol
                    54 // Side
            };

            for (int tag : mandatoryTags) {
                if (expected.isSetField(tag)) {
                    if (!actual.isSetField(tag))
                        return false;
                    if (!expected.getString(tag).equals(actual.getString(tag)))
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
