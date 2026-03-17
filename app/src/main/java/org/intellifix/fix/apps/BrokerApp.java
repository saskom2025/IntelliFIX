package org.intellifix.fix.apps;

import lombok.extern.slf4j.Slf4j;
import org.intellifix.redis.base.MessagePublisher;
import org.intellifix.fix.base.SimulatorAppBase;
import quickfix.*;
import quickfix.field.MsgType;
import quickfix.fix44.ExecutionReport;
import java.util.concurrent.CountDownLatch;

@Slf4j
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

    @Override
    public Message updateTagEleven(Message message, String simId, String senderCompId, String tag) {
        return null;
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
        if (message.isSetField(526)) {
            message.removeField(526);
        }
        String msgType = null;
        try {
            msgType = message.getHeader().getString(MsgType.FIELD);
        } catch (FieldNotFound e) {
            throw new RuntimeException(e);
        }
        messagePublisher.publishMessage("Broker sent 35=" + msgType + " " + pretty(message));
    }

    @Override
    public void fromApp(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {

        String msgType = message.getHeader().getString(MsgType.FIELD);
        messagePublisher.publishMessage("Broker received 35=" + msgType + " " + pretty(message));
        // Try to satisfy expected inbound (D/G/F) when waiting
        Message expected = this.expectedInbound;
        CountDownLatch latch = this.expectedLatch;
        if (expected != null && latch != null) {

            if (matchesExpected(expected, message)) {
                System.out.println("[MATCH] Expected inbound satisfied.");
                latch.countDown();
                this.clearExpectedInbound();
            } else {
                System.out.println("[NO_MATCH_FOUND]");
            }
        }
        crack(message, sessionID);
    }

    public void onMessage(quickfix.fix44.NewOrderSingle order, SessionID sessionID)
            throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
        System.out.println("[BROKER] Received NewOrderSingle. Sending ExecutionReport...");

        // Construct Execution Report using setters to avoid constructor signature
        // issues
        ExecutionReport execReport = new ExecutionReport();
        execReport.set(new quickfix.field.OrderID("ORD" + System.currentTimeMillis()));
        execReport.set(new quickfix.field.ExecID("EXEC" + System.currentTimeMillis()));
        execReport.set(new quickfix.field.ExecType(quickfix.field.ExecType.NEW));
        execReport.set(new quickfix.field.OrdStatus(quickfix.field.OrdStatus.NEW));
        execReport.set(order.getSymbol());
        execReport.set(order.getSide());
        execReport.set(new quickfix.field.LeavesQty(order.getOrderQty().getValue()));
        execReport.set(new quickfix.field.CumQty(0));
        execReport.set(new quickfix.field.AvgPx(0));
        execReport.set(order.getClOrdID());

        if (order.isSetField(526)) {
            execReport.setString(526, order.getString(526));
        }

        try {
            Session.sendToTarget(execReport, sessionID);
        } catch (SessionNotFound e) {
            e.printStackTrace();
        }
    }

    public void onMessage(ExecutionReport report, SessionID sessionID) {
    }

    private boolean matchesExpected(Message expected, Message actual) {
        try {
            System.out.println("EXPECTED: " + expected);
            System.out.println("ACTUAL: " + actual);
            String expType = expected.getHeader().getString(MsgType.FIELD);
            String actType = actual.getHeader().getString(MsgType.FIELD);

            if (!expType.equals(actType))
                return false;
            // multiple tags can be added as comma separated values
            int[] mandatoryTags = new int[] {
                    526
            };

            for (int tag : mandatoryTags) {
                if (expected.isSetField(tag)) {
                    String actualValue = null;
                    if (actual.isSetField(526)) {
                        String val526 = actual.getString(526);
                        actualValue = val526.contains("-") ? val526.substring(val526.lastIndexOf("-") + 1) : val526;
                    } else if (actual.isSetField(tag)) {
                        actualValue = actual.getString(tag);
                    }
                    if (actualValue == null || !expected.getString(tag).equals(actualValue)) {
                        return false;
                    }
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
