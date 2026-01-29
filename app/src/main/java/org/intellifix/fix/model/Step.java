package org.intellifix.fix.model;

import quickfix.Message;

public record Step(StepType type, Message message) {
}
