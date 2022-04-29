package dev.vality.schedulator.service.model;

import dev.vality.damsel.schedule.ExecuteJobRequest;
import dev.vality.damsel.schedule.ScheduledJobContext;
import dev.vality.schedulator.serializer.MachineTimerState;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;

@Data
@Builder
public class ScheduleJobCalculateResult {
    private final ExecuteJobRequest executeJobRequest;

    private final ScheduledJobContext scheduledJobContext;

    private final ByteBuffer remoteJobContext;

    private final MachineTimerState machineTimerState;
}
