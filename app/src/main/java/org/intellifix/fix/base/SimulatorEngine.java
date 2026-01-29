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

    protected abstract StepType determineStepType(String msgType);

    protected abstract void runScenario(List<Step> steps, SimulatorAppBase app, SessionID sid) throws Exception;

    protected abstract void handleOutbound(Message out, SessionID sid) throws Exception;

    protected List<Step> readSteps(String path, DataDictionary dd) throws Exception {
        java.util.concurrent.atomic.AtomicInteger ln = new java.util.concurrent.atomic.AtomicInteger(0);
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            return br.lines()
                    .map(line -> {
                        ln.incrementAndGet();
                        return line.trim();
                    })
                    .filter(line -> !line.isEmpty() && !line.startsWith("#") && !line.startsWith("//"))
                    .map(line -> (line.indexOf('\u0001') < 0 && line.contains("|")) ? line.replace('|', '\u0001')
                            : line)
                    .map(line -> {
                        try {
                            Message msg = new Message(line, dd, false);
                            String msgType = msg.getHeader().getString(MsgType.FIELD);
                            StepType type = determineStepType(msgType);
                            if (type != null) {
                                return new Step(type, msg);
                            } else {
                                log.info("[SKIP] line " + ln.get() + " msgType=" + msgType);
                                return null;
                            }
                        } catch (Exception e) {
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
