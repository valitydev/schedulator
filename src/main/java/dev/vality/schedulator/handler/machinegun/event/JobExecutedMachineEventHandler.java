package dev.vality.schedulator.handler.machinegun.event;

import dev.vality.damsel.domain.BusinessScheduleRef;
import dev.vality.damsel.domain.CalendarRef;
import dev.vality.damsel.schedule.*;
import dev.vality.machinarium.domain.SignalResultData;
import dev.vality.machinarium.domain.TMachine;
import dev.vality.machinarium.domain.TMachineEvent;
import dev.vality.machinegun.msgpack.Value;
import dev.vality.machinegun.stateproc.ComplexAction;
import dev.vality.machinegun.stateproc.HistoryRange;
import dev.vality.schedulator.serializer.MachineRegisterState;
import dev.vality.schedulator.serializer.MachineStateSerializer;
import dev.vality.schedulator.serializer.MachineTimerState;
import dev.vality.schedulator.serializer.SchedulatorMachineState;
import dev.vality.schedulator.service.ScheduleJobService;
import dev.vality.schedulator.service.model.ScheduleJobCalculateResult;
import dev.vality.schedulator.util.TimerActionHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobExecutedMachineEventHandler implements MachineEventHandler {

    private final ScheduleJobService scheduleJobService;

    private final MachineStateSerializer machineStateSerializer;

    @Override
    public SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                        TMachineEvent<ScheduleChange> event) {
        log.info("Process job executed event for machineId: {}", machine.getMachineId());
        if (machine.getMachineState() == null) {
            throw new IllegalStateException("Machine state can't be null");
        }

        // Read current state
        byte[] state = machine.getMachineState().getData().getBin();
        SchedulatorMachineState schedulatorMachineState = machineStateSerializer.deserializer(state);
        MachineRegisterState registerState = schedulatorMachineState.getRegisterState();
        ScheduleJobRegistered scheduleJobRegistered = mapToScheduleJobRegistered(registerState);
        MachineTimerState timerState = schedulatorMachineState.getTimerState();
        log.info("SchedulerJobRegistered: {}, MachineTimerState: {}", scheduleJobRegistered, timerState);

        ScheduleJobCalculateResult scheduleJobCalculateResult =
                scheduleJobService.calculateNextExecutionTime(scheduleJobRegistered, timerState);

        ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(
                new ScheduleJobExecuted(
                        scheduleJobCalculateResult.getExecuteJobRequest(),
                        scheduleJobCalculateResult.getRemoteJobContext()
                )
        );
        ScheduledJobContext scheduledJobContext = scheduleJobCalculateResult.getScheduledJobContext();
        HistoryRange historyRange = TimerActionHelper.buildLastEventHistoryRange();
        ComplexAction complexAction = TimerActionHelper.buildTimerAction(
                scheduledJobContext.getNextFireTime(), historyRange);

        log.info("Next timer action: {}", complexAction);

        // Result machine state
        schedulatorMachineState.setTimerState(scheduleJobCalculateResult.getMachineTimerState());
        byte[] resultState = machineStateSerializer.serialize(schedulatorMachineState);

        SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                Value.bin(resultState),
                Collections.singletonList(scheduleChange),
                complexAction);
        log.debug("Response of processSignalTimeout: {}", signalResultData);

        return signalResultData;
    }

    @Override
    public boolean isHandle(TMachine<ScheduleChange> machine, TMachineEvent<ScheduleChange> event) {
        return event.getData().isSetScheduleJobExecuted();
    }

    private ScheduleJobRegistered mapToScheduleJobRegistered(MachineRegisterState registerState) {
        DominantBasedSchedule dominantBasedSchedule = new DominantBasedSchedule()
                .setBusinessScheduleRef(new BusinessScheduleRef(registerState.getBusinessSchedulerId()))
                .setCalendarRef(new CalendarRef(registerState.getCalendarId()));
        if (registerState.getDominantRevisionId() != null) {
            dominantBasedSchedule.setRevision(registerState.getDominantRevisionId());
        }
        Schedule schedule = Schedule.dominant_schedule(dominantBasedSchedule);
        return new ScheduleJobRegistered()
                .setContext(registerState.getContext().getBytes())
                .setExecutorServicePath(registerState.getExecutorServicePath())
                .setScheduleId(registerState.getSchedulerId())
                .setSchedule(schedule);
    }

}
