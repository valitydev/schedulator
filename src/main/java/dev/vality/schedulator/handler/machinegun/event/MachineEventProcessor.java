package dev.vality.schedulator.handler.machinegun.event;

import dev.vality.damsel.schedule.ScheduleChange;
import dev.vality.machinarium.domain.SignalResultData;
import dev.vality.machinarium.domain.TMachine;
import dev.vality.machinarium.domain.TMachineEvent;

public interface MachineEventProcessor {
    SignalResultData<ScheduleChange> process(TMachine<ScheduleChange> machine,
                                             TMachineEvent<ScheduleChange> machineEvent);
}
