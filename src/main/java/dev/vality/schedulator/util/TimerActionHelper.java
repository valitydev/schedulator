package dev.vality.schedulator.util;

import dev.vality.machinegun.base.Timer;
import dev.vality.machinegun.stateproc.*;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TimerActionHelper {

    public static ComplexAction buildTimerAction(String deadline, HistoryRange historyRange) {
        return new ComplexAction()
                .setTimer(TimerAction.set_timer(new SetTimerAction()
                        .setTimer(Timer.deadline(deadline))
                        .setRange(historyRange)));
    }

    public static ComplexAction buildRemoveAction() {
        ComplexAction complexAction = new ComplexAction();
        RemoveAction removeAction = new RemoveAction();
        complexAction.setRemove(removeAction);

        return complexAction;
    }

    public static HistoryRange buildLastEventHistoryRange() {
        HistoryRange historyRange = new HistoryRange();
        historyRange.setDirection(Direction.backward);
        historyRange.setLimit(1);
        return historyRange;
    }

}
