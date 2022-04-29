package dev.vality.schedulator.cron;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import dev.vality.damsel.base.ScheduleYear;
import dev.vality.damsel.domain.BusinessSchedule;
import dev.vality.damsel.domain.Calendar;
import dev.vality.schedulator.util.SchedulerUtil;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

public class SchedulerCalculator {

    private final Cron cron;

    private final SchedulerCalculatorConfig calculatorConfig;

    private final DateAdjuster dateAdjuster;

    private final TimeZone timeZone;

    private LocalDateTime lastPrevFireTime;
    private LocalDateTime lastNextFireTime;
    private LocalDateTime lastNextCronTime;

    SchedulerCalculator(String cronExpression,
                        Calendar calendar,
                        SchedulerCalculatorConfig calculatorConfig) {
        Objects.requireNonNull(cronExpression, "cronExpression can't be null");
        Objects.requireNonNull(calculatorConfig.getStartTime(), "startTime can't be null");
        this.cron = SchedulerUtil.QUARTZ_CRON_PARSER.parse(cronExpression).validate();
        this.calculatorConfig = calculatorConfig;
        this.dateAdjuster = new ExcludeHolidayAdjuster(calendar, calculatorConfig.getCalendarYear());
        this.timeZone = TimeZone.getTimeZone(calendar.getTimezone());
    }

    public static SchedulerCalculator newSchedulerCalculator(ZonedDateTime startDateTime,
                                                             Calendar calendar,
                                                             BusinessSchedule schedule) {
        List<String> cronList = SchedulerUtil.buildCron(schedule.getSchedule(),
                Optional.ofNullable(calendar.getFirstDayOfWeek()));
        String cron = SchedulerUtil.getNearestCron(cronList, startDateTime);
        Integer year = getYear(schedule.getSchedule().getYear());
        if (schedule.isSetDelay()) {
            SchedulerCalculatorConfig calculatorConfig = SchedulerCalculatorConfig.builder()
                    .calendarYear(year)
                    .startTime(startDateTime.toLocalDateTime())
                    .delayYears(schedule.getDelay().getYears())
                    .delayMonths(schedule.getDelay().getMonths())
                    .delayDays(schedule.getDelay().getDays())
                    .delayHours(schedule.getDelay().getHours())
                    .delayMinutes(schedule.getDelay().getMinutes())
                    .delaySeconds(schedule.getDelay().getSeconds())
                    .build();
            return new SchedulerCalculator(cron, calendar, calculatorConfig);
        } else {
            SchedulerCalculatorConfig calculatorConfig = SchedulerCalculatorConfig.builder()
                    .calendarYear(year)
                    .startTime(startDateTime.toLocalDateTime())
                    .build();
            return new SchedulerCalculator(cron, calendar, calculatorConfig);
        }
    }

    private static Integer getYear(ScheduleYear scheduleYear) {
        if (scheduleYear.isSetEvery()) {
            return LocalDateTime.now().getYear();
        } else if (scheduleYear.isSetOn()) {
            return scheduleYear.getOn().stream()
                    .findFirst()
                    .orElse(LocalDateTime.now().getYear());
        }
        throw new IllegalStateException("Schedule year can't be null");
    }

    public SchedulerComputeResult computeFireTime() {
        if (lastNextFireTime == null) {
            // Compute first fire time
            NextTime nextTime = firstFireTime();
            lastPrevFireTime = nextTime.getPrevFireTime();
            lastNextFireTime = nextTime.getNextFireTime();
            lastNextCronTime = nextTime.getNextCronTime();

            return SchedulerComputeResult.builder()
                    .prevFireTime(lastPrevFireTime.atZone(timeZone.toZoneId()))
                    .nextFireTime(nextTime.getNextFireTime().atZone(timeZone.toZoneId()))
                    .nextCronFireTime(nextTime.getNextCronTime().atZone(timeZone.toZoneId()))
                    .build();
        } else {
            // Compute next fire time
            NextTime nextTime = nextFireTime();
            lastPrevFireTime = lastNextFireTime;
            lastNextFireTime = nextTime.getNextFireTime();
            lastNextCronTime = nextTime.getNextCronTime();

            return SchedulerComputeResult.builder()
                    .prevFireTime(lastPrevFireTime.atZone(timeZone.toZoneId()))
                    .nextFireTime(nextTime.getNextFireTime().atZone(timeZone.toZoneId()))
                    .nextCronFireTime(nextTime.getNextCronTime().atZone(timeZone.toZoneId()))
                    .build();
        }
    }

    private NextTime firstFireTime() {
        LocalDateTime startTime = calculatorConfig.getStartTime();

        LocalDateTime prevFireTime = computePrevFireTime(startTime);

        NextTime nextTime = nextFireTime(prevFireTime.minus(1000, ChronoUnit.MILLIS));
        nextTime.setPrevFireTime(prevFireTime);

        return nextTime;
    }

    private NextTime nextFireTime() {
        return nextFireTime(null);
    }

    private NextTime nextFireTime(LocalDateTime dateTime) {
        NextTime nextDateTime = null;
        do {
            if (nextDateTime != null) {
                nextDateTime = computeNextFireTime(nextDateTime.getNextCronTime());
            } else {
                nextDateTime = computeNextFireTime(dateTime != null ? dateTime : lastNextCronTime);
            }
        } while (nextDateTime.getNextFireTime().equals(
                computeNextFireTime(nextDateTime.getNextCronTime()).getNextFireTime()));


        return nextDateTime;
    }

    private LocalDateTime computePrevFireTime(LocalDateTime dateTime) {
        LocalDateTime result = dateTime;
        result = result.minusYears(calculatorConfig.getDelayYears());
        result = result.minusMonths(calculatorConfig.getDelayMonths());
        if (isDateShift()) {
            result = result.with(dateAdjuster.adjust(
                    -calculatorConfig.getDelayDays(),
                    -calculatorConfig.getDelayHours(),
                    -calculatorConfig.getDelayMinutes(),
                    -calculatorConfig.getDelaySeconds())
            );
        }

        return result;
    }

    private NextTime computeNextFireTime(LocalDateTime dateTime) {
        LocalDateTime nextCronTime = findNextCronExecution(dateTime);
        LocalDateTime nextFireTime = nextCronTime;

        // Exclude holiday only if there is a shift (day, month etc.)
        if (isDateShift()) {
            // skip holiday
            var excludedTime = nextFireTime.toLocalDate().atStartOfDay().with(dateAdjuster.adjust(0, 0, 0, 0));

            nextFireTime = excludedTime.isAfter(nextFireTime) ? excludedTime : nextFireTime;
            nextFireTime = nextFireTime.plusYears(calculatorConfig.getDelayYears());
            nextFireTime = nextFireTime.plusMonths(calculatorConfig.getDelayMonths());
            nextFireTime = nextFireTime.with(dateAdjuster.adjust(
                    calculatorConfig.getDelayDays(),
                    calculatorConfig.getDelayHours(),
                    calculatorConfig.getDelayMinutes(),
                    calculatorConfig.getDelaySeconds()
            ));
        }

        return NextTime.builder()
                .nextFireTime(nextFireTime)
                .nextCronTime(nextCronTime)
                .build();
    }

    private LocalDateTime findNextCronExecution(LocalDateTime date) {
        ZonedDateTime nextCronTime = ExecutionTime.forCron(cron)
                .nextExecution(date.atZone(timeZone.toZoneId()))
                .orElseThrow(() -> {
                    throw new IllegalStateException(
                            String.format("Can't get nextExecution for cron '%s' with '%s'", cron.asString(), date));
                });

        return nextCronTime.toLocalDateTime();
    }

    private boolean isDateShift() {
        return calculatorConfig.getDelayYears() > 0
                || calculatorConfig.getDelayDays() > 0
                || calculatorConfig.getDelayMonths() > 0
                || calculatorConfig.getDelayMinutes() > 0
                || calculatorConfig.getDelayHours() > 0
                || calculatorConfig.getDelaySeconds() > 0;
    }

    @Data
    @Builder
    private static final class NextTime {

        private LocalDateTime prevFireTime;

        private LocalDateTime nextCronTime;

        private LocalDateTime nextFireTime;

    }

}
