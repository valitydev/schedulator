package dev.vality.schedulator.handler.machinegun.event;

import dev.vality.damsel.schedule.ScheduleChange;
import dev.vality.damsel.schedule.ScheduleJobDeregistered;
import dev.vality.machinarium.domain.SignalResultData;
import dev.vality.machinarium.domain.TMachine;
import dev.vality.machinarium.domain.TMachineEvent;
import dev.vality.machinegun.msgpack.Nil;
import dev.vality.machinegun.msgpack.Value;
import dev.vality.machinegun.stateproc.ComplexAction;
import dev.vality.schedulator.util.TimerActionHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class DeregisterMachineEventHandler implements MachineEventHandler {
    @Override
    public SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                        TMachineEvent<ScheduleChange> event) {
        log.info("Process job deregister event for machineId: {}", machine.getMachineId());
        ComplexAction removeAction = TimerActionHelper.buildRemoveAction();

        ScheduleChange jobDeregistered = ScheduleChange.schedule_job_deregistered(new ScheduleJobDeregistered());

        return new SignalResultData<>(Value.nl(new Nil()), List.of(jobDeregistered), removeAction);
    }

    @Override
    public boolean isHandle(TMachine<ScheduleChange> machine, TMachineEvent<ScheduleChange> event) {
        return event.getData().isSetScheduleJobDeregistered();
    }
}
