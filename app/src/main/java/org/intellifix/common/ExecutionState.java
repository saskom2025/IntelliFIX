package org.intellifix.common;

import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutionState {
    public static final AtomicBoolean isFirstMessage = new AtomicBoolean(true);
}
