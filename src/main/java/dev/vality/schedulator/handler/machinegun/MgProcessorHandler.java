package dev.vality.schedulator.handler.machinegun;

import dev.vality.damsel.schedule.*;
import dev.vality.machinarium.domain.CallResultData;
import dev.vality.machinarium.domain.SignalResultData;
import dev.vality.machinarium.domain.TMachine;
import dev.vality.machinarium.domain.TMachineEvent;
import dev.vality.machinarium.handler.AbstractProcessorHandler;
import dev.vality.machinegun.msgpack.Nil;
import dev.vality.machinegun.msgpack.Value;
import dev.vality.machinegun.stateproc.ComplexAction;
import dev.vality.machinegun.stateproc.HistoryRange;
import dev.vality.schedulator.handler.machinegun.event.MachineEventHandler;
import dev.vality.schedulator.handler.machinegun.event.MachineEventProcessor;
import dev.vality.schedulator.service.RemoteClientManager;
import dev.vality.schedulator.service.ScheduleJobService;
import dev.vality.schedulator.service.impl.ScheduleJobCalculateException;
import dev.vality.schedulator.util.TimerActionHelper;
import dev.vality.woody.api.flow.error.WUnavailableResultException;
import dev.vality.woody.api.flow.error.WUndefinedResultException;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class MgProcessorHandler extends AbstractProcessorHandler<ScheduleChange, ScheduleChange> {

    private final RemoteClientManager remoteClientManager;

    private final ScheduleJobService scheduleJobService;

    private final List<MachineEventHandler> machineEventHandlers;

    private final MachineEventProcessor machineEventProcessor;

    public MgProcessorHandler(RemoteClientManager remoteClientManager,
                              ScheduleJobService scheduleJobService,
                              List<MachineEventHandler> machineEventHandlers,
                              MachineEventProcessor machineEventProcessor) {
        super(ScheduleChange.class, ScheduleChange.class);
        this.remoteClientManager = remoteClientManager;
        this.scheduleJobService = scheduleJobService;
        this.machineEventHandlers = machineEventHandlers;
        this.machineEventProcessor = machineEventProcessor;
    }

    @Override
    public final SignalResultData<ScheduleChange> processSignalInit(TMachine machine,
                                                                    ScheduleChange scheduleChangeRegistered) {
        log.info("Request processSignalInit() machineId: {} scheduleChangeRegistered: {}",
                machine.getMachineId(), scheduleChangeRegistered);
        ScheduleJobRegistered jobRegistered = scheduleChangeRegistered.getScheduleJobRegistered();

        // Validate execution context (call remote service)
        ScheduleContextValidated scheduleContextValidated = validateRemoteContext(jobRegistered);

        // Calculate next execution time
        try {
            ScheduleChange changeValidated = ScheduleChange.schedule_context_validated(scheduleContextValidated);
            ScheduledJobContext context = scheduleJobService.calculateScheduledJobContext(jobRegistered);
            HistoryRange historyRange = TimerActionHelper.buildLastEventHistoryRange();
            ComplexAction complexAction = TimerActionHelper.buildTimerAction(context.getNextFireTime(), historyRange);
            log.info("[Signal Init] timer action: {}", complexAction);
            SignalResultData<ScheduleChange> signalResultData = new SignalResultData<>(
                    Value.nl(new Nil()),
                    Arrays.asList(changeValidated, scheduleChangeRegistered),
                    complexAction);
            log.info("Response of processSignalInit: {}", signalResultData);

            return signalResultData;
        } catch (ScheduleJobCalculateException e) {
            log.error("Failed to calculate schedule", e);
            throw new WUndefinedResultException("Failed to calculate schedule", e);
        }
    }

    @Override
    public final SignalResultData<ScheduleChange> processSignalTimeout(
            TMachine<ScheduleChange> machine,
            List<TMachineEvent<ScheduleChange>> machineEventList) {
        try {
            if (machineEventList.isEmpty()) {
                throw new MachineEventHandleException("Machine events can't be empty");
            }
            TMachineEvent<ScheduleChange> machineEvent = machineEventList.get(0); // Expect only one event

            log.info("Request processSignalTimeout() machineId: {} machineEventList: {}",
                    machine.getMachineId(), machineEventList);

            return machineEventProcessor.process(machine, machineEvent);
        } catch (MachineEventHandleException e) {
            log.error("Exception while handle event for machineId = {}", machine, e);
            throw new WUndefinedResultException(e);
        }
    }

    @Override
    public final CallResultData<ScheduleChange> processCall(String namespace,
                                                            String machineId,
                                                            ScheduleChange scheduleChange,
                                                            List<TMachineEvent<ScheduleChange>> machineEvents) {
        log.info("Request processCall() machineId: {} scheduleChange: {} machineEvents: {}",
                machineId, scheduleChange, machineEvents);
        ComplexAction removeAction = TimerActionHelper.buildRemoveAction();
        CallResultData<ScheduleChange> callResultData = new CallResultData<>(
                Value.nl(new Nil()),
                scheduleChange,
                Collections.singletonList(scheduleChange),
                removeAction);
        log.info("Response of processCall: {}", callResultData);
        return callResultData;
    }

    private ScheduleContextValidated validateRemoteContext(ScheduleJobRegistered scheduleJobRegistered) {
        try {
            ByteBuffer contextValidationRequest = ByteBuffer.wrap(scheduleJobRegistered.getContext());
            log.info("Call validation context for '{}'", scheduleJobRegistered.getExecutorServicePath());
            ContextValidationResponse contextValidationResponse = remoteClientManager.validateExecutionContext(
                    scheduleJobRegistered.getExecutorServicePath(), contextValidationRequest);
            log.info("Context validation response: {}", contextValidationResponse);

            return new ScheduleContextValidated(contextValidationRequest, contextValidationResponse);
        } catch (WUnavailableResultException e) {
            log.error("Exception while 'validateExecutionContext'. Failed to call remote service", e);
            throw e;
        } catch (TException e) {
            log.error("Unexpected exception while 'validateExecutionContext'");
            throw new WUndefinedResultException(e);
        }
    }

}
