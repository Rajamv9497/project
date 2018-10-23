#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************APP MENU***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1)${MENU} HIVE ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2)${MENU} PIG ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} MAPREDUCE ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} EXPORT ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU} BAR GRAPH ${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

function getpinCodeBank(){
	echo "in getPinCodebank"
	echo $1
	echo $2
	#hive -e "Select * from AppData where PinCode = $1 AND Bank = '$2'"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
          echo "select the solution that you need"
            echo -e "${MENU}**${NUMBER} 1)${MENU} soln7 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 2)${MENU} soln8 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 3)${MENU} soln9 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 4)${MENU} soln10 ${NORMAL}"
          read  soln
          case $soln in
           1) clear;
            hive -e "select count(case_status),year from niit.final group by year"
            ;;

           2) clear;
            echo "AVG(prevailing_wage) based on"
            echo -e "${MENU}**${NUMBER} 1)${MENU} job_title ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 2)${MENU} year ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 3)${MENU} Both ${NORMAL}"
            read basis
            case $basis in
             1) clear;
                hive -e "select job_title,avg(prevailing_wage),full_time_position from niit.final group by job_title,full_time_position;"
                     ;;                 
             2) clear;
                 hive -e "select year,avg(prevailing_wage),full_time_position from niit.final group by year,full_time_position;"
                     ;;
             3) clear;
             hive -e "select job_title,avg(prevailing_wage),year,full_time_position from niit.final group by job_title,year,full_time_position sort by year;"
            ;;
             *) clear;
                 echo "Invalid option"
                   ;;
             esac
              ;;
           
           3) clear;
            hive -e "select employer_name,round((count(case_status)/1551),2) as counts from niit.final where case_status='CERTIFIED' or case_status='CERTIFIED WITHDRAWN' group by employer_name order by counts desc limit 10;"  
	     ;;
           
           4) clear;
            hive -e "select job_title,round((count(case_status)/1551),2) as counts from niit.final where case_status='CERTIFIED' or case_status='CERTIFIED WITHDRAWN' group by job_title order by counts desc limit 10;"  
	     ;;

           *) echo "Invalid option";;
               esac
               show_menu;
               ;;

        2) clear;
         echo "select the solution that you need"
            echo -e "${MENU}**${NUMBER} 1)${MENU} soln1A ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 2)${MENU} soln1B ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 3)${MENU} soln2A ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 4)${MENU} soln2B ${NORMAL}"
          read  soln
          case $soln in
           1) clear;
              pig -x local /home/rajamv9497/project/pigsolutions/soln1a.pig 
            ;;

           2) clear;
              pig -x local /home/rajamv9497/project/pigsolutions/soln1b.pig 
            ;;
           
           3) clear;
              pig -x local /home/rajamv9497/project/pigsolutions/soln2a.pig 
             ;;
           
           4) clear;
              pig -x local /home/rajamv9497/project/pigsolutions/soln2b.pig   
	     ;;

           *) echo "Invalid option";;
               esac
               show_menu;
               ;;
            
        3) clear;
           echo "select the solution that you need"
            echo -e "${MENU}**${NUMBER} 1)${MENU} soln3 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 2)${MENU} soln4 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 3)${MENU} soln5 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 4)${MENU} soln6 ${NORMAL}"
          read  soln
          case $soln in
           1) clear;
              hadoop jar /home/rajamv9497/Hadooppractice/jarfiles/question3jar.jar question3 file:///home/rajamv9497/project/inputfile/final \final333
              hadoop fs -cat /user/hduser/final333/p*
            ;;

           2) clear;
              hadoop jar /home/rajamv9497/Hadooppractice/jarfiles/question4jar.jar question4 file:///home/rajamv9497/project/inputfile/final \final444
             echo "select the year that you need"
            echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 6)${MENU} all years ${NORMAL}"
          read  year
           case $year in
            1) clear;
                hadoop fs -cat /user/hduser/final444/part-r-00000
                 ;;
            2) clear;
                hadoop fs -cat /user/hduser/final444/part-r-00001
                 ;;
            3) clear;
                hadoop fs -cat /user/hduser/final444/part-r-00002
                  ;;
            4) clear;
                hadoop fs -cat /user/hduser/final444/part-r-00003
                  ;;
            5) clear;
                hadoop fs -cat /user/hduser/final444/part-r-00004
                  ;;
            6) clear;
                hadoop fs -cat /user/hduser/final444/p*
                  ;;
            *) clear;
                 echo "Invalid option"
                  ;;
           esac
           ;;
           
           3) clear;
              hadoop jar /home/rajamv9497/Hadooppractice/jarfiles/question5jar.jar question5 file:///home/rajamv9497/project/inputfile/final \final555
                echo "select the year that you need"
            echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 6)${MENU} all years ${NORMAL}"
          read  year
           case $year in
            1) clear;
                hadoop fs -cat /user/hduser/final555/part-r-00000
                 ;;
            2) clear;
                hadoop fs -cat /user/hduser/final555/part-r-00001
                 ;;
            3) clear;
                hadoop fs -cat /user/hduser/final555/part-r-00002
                  ;;
            4) clear;
                hadoop fs -cat /user/hduser/final555/part-r-00003
                  ;;
            5) clear;
                hadoop fs -cat /user/hduser/final555/part-r-00004
                  ;;
            6) clear;
                hadoop fs -cat /user/hduser/final555/p*
                  ;;
            *) clear;
                 echo "Invalid option"
                  ;;
           esac
           ;;
            
           
           4) clear;
              hadoop jar /home/rajamv9497/Hadooppractice/jarfiles/question6jar.jar question6 file:///home/rajamv9497/project/inputfile/final \final666
                echo "select the year that you need"
            echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
            echo -e "${MENU}**${NUMBER} 6)${MENU} all years ${NORMAL}"
          read  year
           case $year in
            1) clear;
                hadoop fs -cat /user/hduser/final666/part-r-00000
                 ;;
            2) clear;
                hadoop fs -cat /user/hduser/final666/part-r-00001
                 ;;
            3) clear;
                hadoop fs -cat /user/hduser/final666/part-r-00002
                  ;;
            4) clear;
                hadoop fs -cat /user/hduser/final666/part-r-00003
                  ;;
            5) clear;
                hadoop fs -cat /user/hduser/final666/part-r-00004
                  ;;
            6) clear;
                hadoop fs -cat /user/hduser/final666/p*
                  ;;
            *) clear;
                 echo "Invalid option"
                  ;;
           esac
           ;;
            
           *) echo "Invalid option";;
               esac
               show_menu;
               ;;
	
        4) clear;
         echo "EXPORT TO SQL"
           hive -e "insert overwrite directory '/hanoid1'row format delimited fields terminated by ',' select job_title,round((count(case_status)/1551),2) as counts from niit.final where case_status='CERTIFIED' or case_status='CERTIFIED WITHDRAWN' group by job_title order by counts desc limit 10;"
           sqoop export --connect jdbc:mysql://localhost/college --username root --password 'root' --table h1b1 --export-dir /hanoid1/000000_0 --input-fields-terminated-by ',' ;
           mysql -u root -p -D college -e "select * from h1b1 order by perc desc"
       show_menu;
        ;;
	 5) clear;
         echo "1) Question6" 
         echo "2) Question7"      
          read graph 
           case $graph in
            1) clear;
                sudo -H xdg-open /home/rajamv9497/Documents/Chart6.ods        
                  ;;
            2) clear;
                sudo -H xdg-open /home/rajamv9497/Documents/Chart7.ods        
                  ;;
            *) clear;
                 echo "Invalid option"
                  ;;
           esac
           show_menu;
           ;;
              
                 

        \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done


