package org.intellifix.fix;

import lombok.extern.slf4j.Slf4j;
import org.intellifix.fix.base.SimulatorAppBase;
import org.intellifix.fix.base.SimulatorEngine;
import org.intellifix.redis.base.MessagePublisher;
import org.intellifix.redis.RedisMessagePublisher;
import quickfix.*;
import quickfix.field.MsgType;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;
import org.intellifix.fix.apps.BrokerApp;
import org.intellifix.fix.model.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class FixBrokerSimulator extends SimulatorEngine {

    private MessagePublisher messagePublisher;

    public FixBrokerSimulator(MessagePublisher messagePublisher) {
        this.messagePublisher = messagePublisher;
    }

    @Override
    protected StepType determineStepType(Message message) {
        try {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            return switch (msgType) {
                case "D", "G", "F" -> StepType.EXPECT_INBOUND;
                case "8" -> StepType.OUTBOUND;
                default -> null;
            };
        } catch (FieldNotFound e) {
            return null;
        }
    }

    @Override
    protected void runScenario(List<Step> steps, SimulatorAppBase app, SessionID sid) throws Exception {
        for (Step step : steps) {
            switch (step) {
                case Step(StepType type, Message message) when type == StepType.EXPECT_INBOUND -> {
                    System.out.println("Broker Simulator -> EXPECT_INBOUND");
                    CountDownLatch latch = new CountDownLatch(1);
                    app.setExpectedInbound(message, latch);
                    // log.info("[WAIT] for inbound " + pretty(message));
                    boolean isResponseOk = latch.await(120, TimeUnit.SECONDS);
                    app.clearExpectedInbound();
                    if (!isResponseOk) {
                        throw new RuntimeException("Timed out waiting for inbound: " + pretty(message));
                    }
                }
                case Step(StepType type, Message message) when type == StepType.OUTBOUND -> {
                    System.out.println("Broker Simulator -> OUTBOUND");
                    handleOutbound(message, sid);
                    // log.info("[OUT] " + pretty(message));
                    boolean isResponseOk = Session.sendToTarget(message, sid);
                    if (!isResponseOk) {
                        throw new RuntimeException(
                                "Failed to send execution report (Session.sendToTarget returned false)");
                    }
                }
                default -> throw new IllegalStateException("Unsupported step: " + step);
            }
        }
    }

    @Override
    protected void handleOutbound(Message out, SessionID sid) throws Exception {
        System.out.println("#### Broker handleOutbound");
        System.out.println("Message: "+out);
        if (!out.getHeader().isSetField(SenderCompID.FIELD)
                || !out.getHeader().isSetField(TargetCompID.FIELD)) {
            out.getHeader().setString(SenderCompID.FIELD, sid.getSenderCompID());
            out.getHeader().setString(TargetCompID.FIELD, sid.getTargetCompID());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            log.error("[ERROR]: Missing expected inputs");
            System.exit(2);
        }

        MessagePublisher messagePublisher = new RedisMessagePublisher();

        var instance = new FixBrokerSimulator(messagePublisher);
        var settings = new SessionSettings(args[0]);
        var dd = new DataDictionary(args[1]);

        var app = new BrokerApp(messagePublisher);
        var storeFactory = new FileStoreFactory(settings);
        var logFactory = new ScreenLogFactory(true, true, true, true);
        var messageFactory = new DefaultMessageFactory();

        var acceptor = new SocketAcceptor(app, storeFactory, settings, logFactory, messageFactory);
        acceptor.start();
        log.info("[START] Started Acceptor successfully...");

        app.awaitLogon(120);
        var sid = app.getActiveSession();
        if (sid == null)
            throw new RuntimeException("No active session");

        List<Step> steps = instance.readSteps(args[2], dd);
        instance.runScenario(steps, app, sid);

        log.info("[DONE] Scenario completed. Stopping acceptor.");
        acceptor.stop();
    }
}
