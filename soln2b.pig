A = load '/home/rajamv9497/project/inputfile/final' using PigStorage(',') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray, worksite:chararray,workstate:chararray,longitute:double,latitute:double);

A1 = filter A by case_status == 'CERTIFIED';
A2 = group A1 by (workstate,year);
A3 = foreach A2 generate group.year,group,COUNT(A1.case_status) as counts;
A4 = group A3 by year;
A5 = foreach A4 {
      A6 = order A3 by counts desc;
      A7 = limit A6 5;
      generate flatten(A7);
       };
A8 = foreach A5 generate flatten(group),counts;
dump A8;
