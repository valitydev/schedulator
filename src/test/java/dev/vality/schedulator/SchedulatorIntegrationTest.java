package dev.vality.schedulator;

import com.opencsv.exceptions.CsvValidationException;
import dev.vality.damsel.domain.BusinessSchedule;
import dev.vality.damsel.domain.BusinessScheduleRef;
import dev.vality.damsel.domain.Calendar;
import dev.vality.damsel.domain.CalendarRef;
import dev.vality.damsel.schedule.*;
import dev.vality.schedulator.service.DominantService;
import dev.vality.schedulator.service.RemoteClientManager;
import org.apache.thrift.TException;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.Mockito.*;

@Ignore
public class SchedulatorIntegrationTest extends AbstractIntegrationTest {

    @MockBean
    private RemoteClientManager remoteClientManager;

    @MockBean
    private DominantService dominantService;

    @Autowired
    private SchedulatorSrv.Iface schedulatorService;

    @Test
    public void registerAndDeregisterJobTest() throws TException, IOException, CsvValidationException {
        ValidationResponseStatus successValidationStatus = new ValidationResponseStatus();
        successValidationStatus.setSuccess(new ValidationSuccess());
        when(remoteClientManager.validateExecutionContext(anyString(), any(ByteBuffer.class)))
                .thenReturn(new ContextValidationResponse(successValidationStatus));
        BusinessSchedule businessSchedule = ScheduleTestData.buildSchedule(2020, null, null, null, null, null, null);
        when(dominantService.getBusinessSchedule(any(BusinessScheduleRef.class), anyLong()))
                .thenReturn(businessSchedule);
        Calendar calendar = ScheduleTestData.buildTestCalendar();
        when(dominantService.getCalendar(any(CalendarRef.class), anyLong())).thenReturn(calendar);

        RegisterJobRequest registerJobRequest = ScheduleTestData.buildRegisterJobRequest();

        schedulatorService.registerJob("testJob", registerJobRequest);
        schedulatorService.deregisterJob("testJob");
    }

}
