package dev.vality.schedulator;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import dev.vality.damsel.base.*;
import dev.vality.damsel.domain.*;
import dev.vality.damsel.schedule.DominantBasedSchedule;
import dev.vality.damsel.schedule.RegisterJobRequest;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ScheduleTestData {

    public static RegisterJobRequest buildRegisterJobRequest() {
        RegisterJobRequest registerJobRequest = new RegisterJobRequest();
        registerJobRequest.setContext(ByteBuffer.wrap(new byte[]{}));
        registerJobRequest.setExecutorServicePath("testUrl");
        registerJobRequest.setSchedule(
                dev.vality.damsel.schedule.Schedule.dominant_schedule(buildDominantSchedule()));

        return registerJobRequest;
    }

    public static DominantBasedSchedule buildDominantSchedule() {
        DominantBasedSchedule dominantBasedSchedule = new DominantBasedSchedule();
        dominantBasedSchedule.setRevision(1);
        dominantBasedSchedule.setBusinessScheduleRef(new BusinessScheduleRef(1));
        dominantBasedSchedule.setCalendarRef(new CalendarRef(1));
        dev.vality.damsel.schedule.Schedule.dominant_schedule(dominantBasedSchedule);

        return dominantBasedSchedule;
    }

    public static Calendar buildTestCalendar() throws IOException, CsvValidationException {
        Calendar calendar = new Calendar();
        calendar.setName("test-calendar");
        calendar.setTimezone("Europe/Moscow");

        ClassPathResource resource = new ClassPathResource("/data/calendar-test.csv");
        try (CSVReader reader = new CSVReader(new InputStreamReader(resource.getInputStream()))) {
            reader.readNext(); // Ignore first line

            String[] row;
            Map<Integer, Set<CalendarHoliday>> years = new HashMap<>();
            while ((row = reader.readNext()) != null) {
                Set<CalendarHoliday> calendarHolidays = new HashSet<>();

                int year = Integer.parseInt(row[0]);
                years.put(year, calendarHolidays);

                for (Month month : Month.values()) {
                    for (String day : row[month.getValue()].split(",")) {
                        if (day.endsWith("*")) {
                            continue;
                        } // Ignore short work day
                        CalendarHoliday holiday = new CalendarHoliday();
                        holiday.setName("holiday");
                        holiday.setDay(Byte.parseByte(day));
                        holiday.setMonth(month);
                        calendarHolidays.add(holiday);
                    }
                }
            }
            calendar.setHolidays(years);
        }
        return calendar;
    }

    public static BusinessSchedule buildSchedule(Integer year,
                                                 Month month,
                                                 Byte dayOfMonth,
                                                 DayOfWeek dayOfWeek,
                                                 Byte hour,
                                                 Byte minute,
                                                 Byte second) {
        dev.vality.damsel.base.Schedule schedule = new dev.vality.damsel.base.Schedule();

        ScheduleYear scheduleYear = new ScheduleYear();
        if (year == null) {
            scheduleYear.setEvery(new ScheduleEvery());
        } else {
            scheduleYear.setOn(Set.of(year));
        }
        schedule.setYear(scheduleYear);

        ScheduleMonth scheduleMonth = new ScheduleMonth();
        if (month == null) {
            scheduleMonth.setEvery(new ScheduleEvery());
        } else {
            scheduleMonth.setOn(Set.of(month));
        }
        schedule.setMonth(scheduleMonth);

        schedule.setDayOfMonth(buildScheduleDayOfMonth(dayOfMonth));

        schedule.setDayOfWeek(buildScheduleDayOfWeek(dayOfWeek));

        schedule.setHour(buildScheduleHour(hour));

        schedule.setMinute(buildScheduleMinute(minute));

        schedule.setSecond(buildScheduleSecond(second));

        BusinessSchedule businessSchedule = new BusinessSchedule();
        businessSchedule.setSchedule(schedule);

        return businessSchedule;
    }

    private static ScheduleFragment buildScheduleDayOfMonth(Byte dayOfMonth) {
        ScheduleFragment scheduleDayOfMonth = new ScheduleFragment();
        if (dayOfMonth == null) {
            scheduleDayOfMonth.setEvery(new ScheduleEvery());
        } else {
            scheduleDayOfMonth.setOn(Set.of(dayOfMonth));
        }

        return scheduleDayOfMonth;
    }

    private static ScheduleDayOfWeek buildScheduleDayOfWeek(DayOfWeek dayOfWeek) {
        ScheduleDayOfWeek scheduleDayOfWeek = new ScheduleDayOfWeek();
        if (dayOfWeek == null) {
            scheduleDayOfWeek.setEvery(new ScheduleEvery());
        } else {
            scheduleDayOfWeek.setOn(Set.of(dayOfWeek));
        }

        return scheduleDayOfWeek;
    }

    private static ScheduleFragment buildScheduleHour(Byte hour) {
        ScheduleFragment scheduleHour = new ScheduleFragment();
        if (hour == null) {
            scheduleHour.setEvery(new ScheduleEvery());
        } else {
            scheduleHour.setOn(Set.of(hour));
        }

        return scheduleHour;
    }

    private static ScheduleFragment buildScheduleMinute(Byte minute) {
        ScheduleFragment scheduleMinute = new ScheduleFragment();
        if (minute == null) {
            scheduleMinute.setEvery(new ScheduleEvery());
        } else {
            scheduleMinute.setOn(Set.of(minute));
        }

        return scheduleMinute;
    }

    private static ScheduleFragment buildScheduleSecond(Byte second) {
        ScheduleFragment scheduleSecond = new ScheduleFragment();
        if (second == null) {
            scheduleSecond.setEvery(new ScheduleEvery());
        } else {
            scheduleSecond.setOn(Set.of(second));
        }

        return scheduleSecond;
    }

}
