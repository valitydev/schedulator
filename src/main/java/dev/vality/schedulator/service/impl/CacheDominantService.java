package dev.vality.schedulator.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import dev.vality.damsel.domain.BusinessSchedule;
import dev.vality.damsel.domain.BusinessScheduleRef;
import dev.vality.damsel.domain.Calendar;
import dev.vality.damsel.domain.CalendarRef;
import dev.vality.schedulator.exception.NotFoundException;
import dev.vality.schedulator.service.DominantService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
@RequiredArgsConstructor
public class CacheDominantService implements DominantService {

    private final Cache<Long, Calendar> dominantCache;

    private final DominantServiceImpl dominantService;

    @Override
    public BusinessSchedule getBusinessSchedule(BusinessScheduleRef scheduleRef, Long domainRevision)
            throws NotFoundException {
        return dominantService.getBusinessSchedule(scheduleRef, domainRevision);
    }

    @Override
    public Calendar getCalendar(CalendarRef calendarRef, Long domainRevision) throws NotFoundException {
        long key = calendarRef.getId() + (domainRevision != null ? domainRevision : 0);
        return dominantCache.get(key, s -> dominantService.getCalendar(calendarRef, domainRevision));
    }
}
