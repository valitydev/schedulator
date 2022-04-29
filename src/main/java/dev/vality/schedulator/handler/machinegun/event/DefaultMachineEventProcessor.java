package dev.vality.schedulator.handler.machinegun.event;

import dev.vality.damsel.schedule.ScheduleChange;
import dev.vality.machinarium.domain.SignalResultData;
import dev.vality.machinarium.domain.TMachine;
import dev.vality.machinarium.domain.TMachineEvent;
import dev.vality.schedulator.handler.machinegun.MachineEventHandleException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class DefaultMachineEventProcessor implements MachineEventProcessor {

    private final List<MachineEventHandler> machineEventHandlers;

    @Override
    public SignalResultData<ScheduleChange> process(TMachine<ScheduleChange> machine,
                                                    TMachineEvent<ScheduleChange> machineEvent) {
        for (MachineEventHandler machineEventHandler : machineEventHandlers) {
            if (machineEventHandler.isHandle(machine, machineEvent)) {
                try {
                    return machineEventHandler.handleEvent(machine, machineEvent);
                } catch (Exception e) {
                    throw new MachineEventHandleException("Exception during handle machine event", e);
                }
            }
        }
        throw new MachineEventHandleException(String.format("Not found handler for event '%s'", machineEvent));
    }
}
