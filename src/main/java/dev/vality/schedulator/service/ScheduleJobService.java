package dev.vality.schedulator.service;

import dev.vality.damsel.schedule.ScheduleJobRegistered;
import dev.vality.damsel.schedule.ScheduledJobContext;
import dev.vality.schedulator.serializer.MachineTimerState;
import dev.vality.schedulator.service.model.ScheduleJobCalculateResult;

public interface ScheduleJobService {

    ScheduleJobCalculateResult calculateNextExecutionTime(ScheduleJobRegistered scheduleJobRegistered,
                                                          MachineTimerState machineTimer);

    ScheduledJobContext calculateScheduledJobContext(ScheduleJobRegistered scheduleJobRegistered);

}
