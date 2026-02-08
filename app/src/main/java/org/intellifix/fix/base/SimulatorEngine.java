package org.intellifix.fix.base;

import lombok.extern.slf4j.Slf4j;
import quickfix.*;
import quickfix.field.MsgType;
import org.intellifix.fix.model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

@Slf4j
public abstract class SimulatorEngine {

    protected abstract StepType determineStepType(Message message);

    protected abstract void runScenario(List<Step> steps, SimulatorAppBase app, SessionID sid) throws Exception;

    protected abstract void handleOutbound(Message out, SessionID sid) throws Exception;

    protected List<Step> readSteps(String path, DataDictionary dd, SessionID sid) throws Exception {
        java.util.concurrent.atomic.AtomicInteger ln = new java.util.concurrent.atomic.AtomicInteger(0);
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            return br.lines()
                    .map(line -> {
                        ln.incrementAndGet();
                        return line.trim();
                    })
                    .filter(line -> !line.isEmpty() && !line.startsWith("#") && !line.startsWith("//"))
                    .map(line -> line.replaceFirst("^\\d+:\\s*", ""))
                    .map(line -> (line.indexOf('\u0001') < 0 && line.contains("|")) ? line.replace('|', '\u0001')
                            : line)
                    .map(line -> {
                        try {
                            if (!line.endsWith("\u0001")) {
                                line += "\u0001";
                            }
                            Message msg = new Message(line, dd, false);

                            String msgSender = msg.getHeader().getString(49);
                            String msgTarget = msg.getHeader().getString(56);
                            String ourSender = sid.getSenderCompID();

                            StepType type = determineStepType(msg);
                            if (type == null) {
                                String msgType = msg.getHeader().getString(MsgType.FIELD);
                                log.info("[SKIP] line " + ln.get() + " msgType=" + msgType
                                        + " - Not handled by simulator");
                                return null;
                            }

                            if (type == StepType.OUTBOUND) {
                                if (!msgSender.equals(ourSender)) {
                                    log.info("[SKIP] line " + ln.get()
                                            + " - OUTBOUND message has different SenderCompID: " + msgSender);
                                    return null;
                                }
                            } else if (type == StepType.EXPECT_INBOUND) {
                                if (!msgTarget.equals(ourSender)) {
                                    log.info("[SKIP] line " + ln.get()
                                            + " - EXPECT_INBOUND message has different TargetCompID: " + msgTarget);
                                    return null;
                                }
                            }

                            return new Step(type, msg);
                        } catch (Exception e) {
                            log.error("Failed to parse line {}: {}", ln.get(), line, e);
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(java.util.Objects::nonNull)
                    .toList();
        }
    }

    protected String pretty(Message m) {
        return m.toString().replace('\u0001', '|');
    }
}
