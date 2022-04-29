package dev.vality.schedulator.service.impl;

import dev.vality.damsel.domain.BusinessSchedule;
import dev.vality.damsel.domain.BusinessScheduleRef;
import dev.vality.damsel.domain.Calendar;
import dev.vality.damsel.domain.CalendarRef;
import dev.vality.damsel.domain_config.*;
import dev.vality.schedulator.exception.NotFoundException;
import dev.vality.schedulator.service.DominantService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DominantServiceImpl implements DominantService {

    private final RepositoryClientSrv.Iface dominantClient;
    private final RetryTemplate retryTemplate;

    @Override
    public BusinessSchedule getBusinessSchedule(BusinessScheduleRef scheduleRef, Long domainRevision) {
        var revisionReference = domainRevision != null ? Reference.version(domainRevision) : Reference.head(new Head());
        log.debug("Trying to get schedule, scheduleRef='{}', revisionReference='{}'", scheduleRef, revisionReference);
        try {
            var reference = new dev.vality.damsel.domain.Reference();
            reference.setBusinessSchedule(scheduleRef);
            VersionedObject versionedObject = checkoutObject(revisionReference, reference);
            BusinessSchedule schedule = versionedObject.getObject().getBusinessSchedule().getData();
            log.debug("Schedule has been found, scheduleRef='{}', revisionReference='{}', schedule='{}'",
                    scheduleRef, revisionReference, schedule);
            return schedule;
        } catch (VersionNotFound | ObjectNotFound ex) {
            throw new NotFoundException(String.format("Version not found, scheduleRef='%s', revisionReference='%s'",
                    scheduleRef, revisionReference), ex);
        } catch (TException ex) {
            throw new RuntimeException(String.format("Failed to get schedule, scheduleRef='%s', revisionReference='%s'",
                    scheduleRef, revisionReference), ex);
        }
    }

    @Override
    public Calendar getCalendar(CalendarRef calendarRef, Long domainRevision) {
        var revisionReference = domainRevision != null ? Reference.version(domainRevision) : Reference.head(new Head());
        log.debug("Trying to get calendar, calendarRef='{}', revisionReference='{}'", calendarRef, revisionReference);
        try {
            dev.vality.damsel.domain.Reference reference = new dev.vality.damsel.domain.Reference();
            reference.setCalendar(calendarRef);
            VersionedObject versionedObject = checkoutObject(revisionReference, reference);
            Calendar calendar = versionedObject.getObject().getCalendar().getData();
            log.debug("Calendar has been found, calendarRef='{}', revisionReference='{}', calendar='{}'",
                    calendarRef, revisionReference, calendar);
            return calendar;
        } catch (VersionNotFound | ObjectNotFound ex) {
            throw new NotFoundException(String.format("Version not found, calendarRef='%s', revisionReference='%s'",
                    calendarRef, revisionReference), ex);
        } catch (TException ex) {
            throw new RuntimeException(String.format("Failed to get calendar, calendarRef='%s', revisionReference='%s'",
                    calendarRef, revisionReference), ex);
        }
    }

    private VersionedObject checkoutObject(Reference revisionReference, dev.vality.damsel.domain.Reference reference)
            throws TException {
        return retryTemplate.execute(
                context -> dominantClient.checkoutObject(revisionReference, reference)
        );
    }
}
