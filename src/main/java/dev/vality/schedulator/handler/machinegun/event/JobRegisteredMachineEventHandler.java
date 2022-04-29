package dev.vality.schedulator.handler.machinegun.event;

import dev.vality.damsel.schedule.ScheduleChange;
import dev.vality.damsel.schedule.ScheduleJobExecuted;
import dev.vality.damsel.schedule.ScheduleJobRegistered;
import dev.vality.damsel.schedule.ScheduledJobContext;
import dev.vality.machinarium.domain.SignalResultData;
import dev.vality.machinarium.domain.TMachine;
import dev.vality.machinarium.domain.TMachineEvent;
import dev.vality.machinegun.msgpack.Value;
import dev.vality.machinegun.stateproc.ComplexAction;
import dev.vality.machinegun.stateproc.HistoryRange;
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
public class JobRegisteredMachineEventHandler implements MachineEventHandler {

    private final ScheduleJobService scheduleJobService;

    private final MachineStateSerializer machineStateSerializer;

    @Override
    public SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine,
                                                        TMachineEvent<ScheduleChange> event) {
        log.info("Process job register event (time calculation) for machineId: {}", machine.getMachineId());
        ScheduleJobRegistered scheduleJobRegistered = event.getData().getScheduleJobRegistered();
        try {
            log.info("Handle register schedule machine event: {}", scheduleJobRegistered);

            // Calculate next execution time
            ScheduleJobCalculateResult scheduleJobCalculateResult =
                    scheduleJobService.calculateNextExecutionTime(scheduleJobRegistered, null);

            // Build timeout signal result
            ScheduledJobContext scheduledJobContext = scheduleJobCalculateResult.getScheduledJobContext();
            HistoryRange historyRange = TimerActionHelper.buildLastEventHistoryRange();
            ComplexAction complexAction = TimerActionHelper.buildTimerAction(
                    scheduledJobContext.getNextFireTime(), historyRange);

            log.info("Next timer action: {}", complexAction);

            SchedulatorMachineState schedulatorMachineState = new SchedulatorMachineState(scheduleJobRegistered);
            schedulatorMachineState.setTimerState(new MachineTimerState());
            byte[] state = machineStateSerializer.serialize(schedulatorMachineState);

            log.info("Schedulator machine state: {}", schedulatorMachineState);

            ScheduleChange scheduleChange = ScheduleChange.schedule_job_executed(
                    new ScheduleJobExecuted(scheduleJobCalculateResult.getExecuteJobRequest(),
                            scheduleJobCalculateResult.getRemoteJobContext())
            );
            SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                    Value.bin(state),
                    Collections.singletonList(scheduleChange),
                    complexAction);
            log.info("Response of processSignalTimeout: {}", signalResultData);

            return signalResultData;
        } catch (Exception e) {
            String errMsg = String.format("Unexpected exception during handle '%s' schedule for '%s'",
                    scheduleJobRegistered.getScheduleId(), scheduleJobRegistered.getExecutorServicePath());
            throw new EventHandlerException(errMsg, e);
        }
    }

    @Override
    public boolean isHandle(TMachine<ScheduleChange> machine, TMachineEvent<ScheduleChange> event) {
        return event.getData().isSetScheduleJobRegistered();
    }

}
