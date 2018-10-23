A = load '/home/rajamv9497/project/inputfile/final' using PigStorage(',') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray, worksite:chararray,longitute:double,latitute:double);

A1 = filter A by job_title == 'DATA ENGINEER';
A2 = group A1 by year;
A3 = foreach A2 generate group as A2,COUNT(A1.job_title);
dump A3;

