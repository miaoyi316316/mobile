package com.miao.logmobile.parser.modle.dim.base;



import com.miao.logmobile.common.DateTypeEnum;
import com.miao.logmobile.timeTransform.TimeTransform;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

import static com.miao.logmobile.common.DateTypeEnum.YEAR;

public class DateDimension  extends BaseDimension{

    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private Date  calendar = new Date();
    private String type;

    public DateDimension(){
        super();
    }


    public DateDimension(int year, int season, int month, int week, int day) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
    }

    public DateDimension(int year, int season, int month, int week, int day, Date calendar) {
        this(year, season, month, week, day);
        this.calendar = calendar;

    }

    public DateDimension(int year, int season, int month, int week, int day, Date calendar, String type) {
        this(year, season, month, week, day, calendar);

        this.type = type;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return id == that.id &&
                year == that.year &&
                season == that.season &&
                month == that.month &&
                week == that.week &&
                day == that.day &&
                Objects.equals(calendar, that.calendar) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, year, season, month, week, day, calendar, type);
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }




    @Override
    public int compareTo(BaseDimension o) {
        if(this==o){
            return 0;
        }
        DateDimension other = (DateDimension) o;

        int temp = this.id - other.id;
        if(temp!=0){
            return temp;
        }else {
            temp = Integer.compare(this.year, other.year);
            if(temp!=0){
                return temp;
            }else {
                temp = Integer.compare(this.season, other.season);
                if(temp!=0){
                    return temp;
                }else {
                    temp = Integer.compare(this.month, other.month);
                    if(temp!=0){
                        return temp;
                    }else {
                        temp = Integer.compare(this.week, other.week);
                        if(temp!=0){
                            return temp;
                        }else {
                            temp = Integer.compare(this.day, other.day);
                            return temp != 0 ? temp : this.type.compareTo(other.type);
                        }
                    }
                }
            }
        }

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.season);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.week);
        dataOutput.writeInt(this.day);
        dataOutput.writeLong(this.calendar.getTime());
        dataOutput.writeUTF(this.type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.season = dataInput.readInt();
        this.month = dataInput.readInt();
        this.week = dataInput.readInt();
        this.day = dataInput.readInt();
        this.calendar.setTime(dataInput.readLong());
        this.type = dataInput.readUTF();
    }


    public static DateDimension buildDate(long timestamp, DateTypeEnum type) {

        DateDimension dateDimension =null;
        Calendar calendar = Calendar.getInstance();
        calendar.clear();

        int year = 0;
        int season = 0;
        int month = 0;
        int week = 0;
        int day = 0;


        switch (type){
            //年指标，需要当年的一月一日
            case YEAR:{
                year = TimeTransform.getDateInfo(timestamp,DateTypeEnum.YEAR);
                calendar.set(year,0,1);
                dateDimension = new DateDimension(year, 0, 0, 0, 0,calendar.getTime(),type.getType());
                break;
            }
            //季度指标，需要当季度的第一个月，第一天
            case SEASON:{
                 year = TimeTransform.getDateInfo(timestamp, DateTypeEnum.YEAR);
                 season = TimeTransform.getDateInfo(timestamp, DateTypeEnum.SEASON);
                 month = season * 3 - 2;
                calendar.set(year,month-1,1);
                dateDimension = new DateDimension(year, season, 0,0,0,calendar.getTime(),type.getType());
                break;
            }
            //月指标，需要当月的第一天
            case MONTH:{
                 year = TimeTransform.getDateInfo(timestamp, DateTypeEnum.YEAR);
                 season = TimeTransform.getDateInfo(timestamp, DateTypeEnum.SEASON);
                 month = TimeTransform.getDateInfo(timestamp, DateTypeEnum.MONTH);
                calendar.set(year,month-1,1);
                dateDimension = new DateDimension(year, season, month, 0, 0,calendar.getTime(),type.getType());
                break;

            }
            //周指标，需要当周的第一天
            case WEEK:{
                year = TimeTransform.getDateInfo(timestamp, DateTypeEnum.YEAR);
                season = TimeTransform.getDateInfo(timestamp, DateTypeEnum.SEASON);
                month = TimeTransform.getDateInfo(timestamp, DateTypeEnum.MONTH);
                week = TimeTransform.getDateInfo(timestamp, DateTypeEnum.WEEK);
                int firstDayOfWeek = TimeTransform.getFirstDayOfWeek(timestamp);
                calendar.set(year,month-1,firstDayOfWeek);
                dateDimension = new DateDimension(year, season, month, week, 0,calendar.getTime(),type.getType());
                break;
            }
            case DAY:{
                year = TimeTransform.getDateInfo(timestamp, DateTypeEnum.YEAR);
                season = TimeTransform.getDateInfo(timestamp, DateTypeEnum.SEASON);
                month = TimeTransform.getDateInfo(timestamp, DateTypeEnum.MONTH);
                week = TimeTransform.getDateInfo(timestamp, DateTypeEnum.WEEK);
                day = TimeTransform.getDateInfo(timestamp,DateTypeEnum.DAY);
                calendar.set(year,month-1,day);
                dateDimension = new DateDimension(year, season, month, week, day,calendar.getTime(),type.getType());
                break;
            }
        }


        return dateDimension;

    }

    @Override
    public String toString() {
        return "DateDimension{" +
                ", year=" + year +
                ", season=" + season +
                ", month=" + month +
                ", week=" + week +
                ", day=" + day +
                ", calendar=" + calendar +
                ", type='" + type + '\'' +
                '}';
    }
}
