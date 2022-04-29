package dev.vality.schedulator.serializer;

import lombok.Data;

@Data
public class MachineTimerState {

    private int jobRetryCount;

    private long currentInterval = -1;

}
