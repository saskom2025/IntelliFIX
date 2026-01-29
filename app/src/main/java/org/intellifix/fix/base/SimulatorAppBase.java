package org.intellifix.fix.base;

import quickfix.Message;
import quickfix.SessionID;
import java.util.concurrent.CountDownLatch;

public interface SimulatorAppBase {
    void awaitLogon(long timeoutSeconds) throws InterruptedException;

    void setExpectedInbound(Message expected, CountDownLatch latch);

    void clearExpectedInbound();

    SessionID getActiveSession();
}
