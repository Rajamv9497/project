A = load '/home/rajamv9497/project/inputfile/final' using PigStorage(',') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray, worksite:chararray,workstate:chararray,longitute:double,latitute:double);

A1 = group A by (year,job_title);
A2 = foreach A1 generate group.year,group,COUNT(A.case_status) as counts;
A3 = group A2 by year;
A4 = foreach A3 {
      A5 = order A2 by counts desc;
      A6 = limit A5 5;
      generate flatten(A6);
       };
A7 = foreach A4 generate flatten(group),counts;
dump A7;
