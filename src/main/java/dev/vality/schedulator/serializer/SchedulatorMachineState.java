package dev.vality.schedulator.serializer;

import dev.vality.damsel.schedule.DominantBasedSchedule;
import dev.vality.damsel.schedule.ScheduleJobRegistered;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SchedulatorMachineState {

    private MachineRegisterState registerState;

    private MachineTimerState timerState;

    public SchedulatorMachineState(ScheduleJobRegistered scheduleJobRegistered) {
        MachineRegisterState registerState = new MachineRegisterState();
        registerState.setContext(new RegisterContext(scheduleJobRegistered.getContext()));
        registerState.setExecutorServicePath(scheduleJobRegistered.getExecutorServicePath());
        registerState.setSchedulerId(scheduleJobRegistered.getScheduleId());
        DominantBasedSchedule dominantSchedule = scheduleJobRegistered.getSchedule().getDominantSchedule();
        registerState.setDominantRevisionId(dominantSchedule.isSetRevision() ? dominantSchedule.getRevision() : null);
        registerState.setBusinessSchedulerId(dominantSchedule.getBusinessScheduleRef().getId());
        registerState.setCalendarId(dominantSchedule.getCalendarRef().getId());
        this.registerState = registerState;
    }

}
