package dev.vality.schedulator.service;

import dev.vality.damsel.domain.BusinessSchedule;
import dev.vality.damsel.domain.BusinessScheduleRef;
import dev.vality.damsel.domain.Calendar;
import dev.vality.damsel.domain.CalendarRef;
import dev.vality.schedulator.exception.NotFoundException;

public interface DominantService {
    BusinessSchedule getBusinessSchedule(BusinessScheduleRef scheduleRef, Long domainRevision) throws NotFoundException;

    Calendar getCalendar(CalendarRef calendarRef, Long domainRevision) throws NotFoundException;
}
