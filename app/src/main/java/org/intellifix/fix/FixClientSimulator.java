package org.intellifix.fix;

import lombok.extern.slf4j.Slf4j;
import org.intellifix.fix.base.SimulatorAppBase;
import org.intellifix.fix.base.SimulatorEngine;
import org.intellifix.redis.base.MessagePublisher;
import org.intellifix.redis.RedisMessagePublisher;
import quickfix.*;
import org.intellifix.fix.apps.ClientApp;
import org.intellifix.fix.model.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class FixClientSimulator extends SimulatorEngine {

    private MessagePublisher messagePublisher;

    public FixClientSimulator(MessagePublisher messagePublisher) {
        this.messagePublisher = messagePublisher;
    }

    @Override
    protected StepType determineStepType(String msgType) {
        return switch (msgType) {
            case "D", "G", "F" -> StepType.OUTBOUND;
            case "8" -> StepType.EXPECT_INBOUND;
            default -> null;
        };
    }

    @Override
    protected void runScenario(List<Step> steps, SimulatorAppBase app, SessionID sid) throws Exception {
        for (Step step : steps) {
            switch (step) {
                case Step(StepType type, Message message) when type == StepType.OUTBOUND -> {
                    // handleOutbound(message, sid);
                    boolean isResponseOk = Session.sendToTarget(message, sid);
                    if (!isResponseOk) {
                        throw new RuntimeException(
                                "Failed to send message to target (Session.sendToTarget returned false)");
                    }
                }
                case Step(StepType type, Message message) when type == StepType.EXPECT_INBOUND -> {
                    CountDownLatch latch = new CountDownLatch(1);
                    app.setExpectedInbound(message, latch);
                    // System.out.println("[WAIT] for inbound " + pretty(message));
                    boolean isResponseOk = latch.await(60, TimeUnit.SECONDS);
                    app.clearExpectedInbound();
                    if (!isResponseOk) {
                        throw new RuntimeException(
                                "Timed out waiting for expected inbound message: " + pretty(message));
                    }
                }
                default -> throw new IllegalStateException("Unsupported step: " + step);
            }
        }
    }

    @Override
    protected void handleOutbound(Message message, SessionID sid) throws Exception {
        /*
         * String msgType = message.getHeader().getString(MsgType.FIELD);
         * messagePublisher.publishMessage("[OUT] 35=" + msgType + " " +
         * pretty(message));
         */
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            log.error("[ERROR]: Missing expected inputs");
            System.exit(2);
        }
        var messagePublisher = new RedisMessagePublisher();
        var instance = new FixClientSimulator(messagePublisher);
        var settings = new SessionSettings(args[0]);
        var dd = new DataDictionary(args[1]);

        var app = new ClientApp(settings, dd, messagePublisher);
        var storeFactory = new FileStoreFactory(settings);
        var logFactory = new ScreenLogFactory(true, true, true, true);
        var messageFactory = new DefaultMessageFactory();

        var initiator = new SocketInitiator(app, storeFactory, settings, logFactory, messageFactory);
        initiator.start();
        log.info("[START] Started Initiator successfully...");

        app.awaitLogon(60);
        var sid = app.getActiveSession();
        if (sid == null)
            throw new RuntimeException("No active session ID");

        List<Step> steps = instance.readSteps(args[2], dd);
        instance.runScenario(steps, app, sid);

        log.info("[DONE] Scenario completed. Stopping initiator.");
        initiator.stop();
    }
}
