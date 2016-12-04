# HDP_Test_Q2
Output data of hadoop test question 2


Q2:
Find out Salary trends between Male and Female employees during each decade in every department. The output should be in the following format: Department, Decade, Gender, avg_salary. Please upload the final CSV along with the code and jar files(if any) to your github and provide the link below.


To get this done I have performed  and used "Hive" and "Pig" with query which get the Department name, Decade , Gender and Average salary.


** Using Hive first I get the employee records with department wise, decade wise and mentioned gender wise with salary.

** Then the Hive Out put file used in Pig to calculate the average salary of department wise , decade wise and gender wise.


In this question I have First get the All Department types.
Then the term "Decade" indicates that 10 years of span, for this I have checked the all department data and employess from, to date data and as analysis found that the data available from 1985.
Here I have created decades in below format :


 1. 1981 to 1990
 2. 1991 to 2000
 3. 2001 to 2010
 4. 2011 to 2020
 
 Then jumped to next required data as department wise, each decade wise  and then gender wise.
 
 Here in available data we have Gender Male (M) and Female (F).
 
 ** The Query used for this question to get the out put below:
 
  1.
  //Below command/quesry use to genrate/get csv file of the male users who worked under the sales department on in the decade of 1991 to 2000

INSERT OVERWRITE LOCAL DIRECTORY '/home/vishnu/Documents/sales_male_1991_2000' row format delimited fields terminated by ',' select d.dept_name,'1991-2000' as col3,e.gender,s.salary,de.from_date from employees e left join salaries s on (s.emp_no = e.emp_no) left join dept_emp1 de on (e.emp_no = de.emp_no) left join departments d on(d.dept_no = de.dept_no) where (de.from_date between 1991 AND 2000 OR de.to_date between 1991 AND 2000) AND e.gender LIKE 'M' AND d.dept_name LIKE '%Sales%' order by de.from_date;

 
In Above query first I have added the  file path to store the result of this file then 

1. First we are taking department name 
2.Then Given the decade year values.
3.Taken Gender , salary and from_date column from employees table 
4.Here made left join on Salaries using the emp.no column from employee and salaries table.
5.Then again I ahve made left join on dept_emp1 table on emp.no column
6.This countinues and made another one left join on departments table on emp.no column
7.Now used where clause and  added condition to get the employee on the froma dn to date basis 
8.Then checked with Gender by pasisng value using LIKE "M"
9.Then for department name using LIKE "%Department Name%.

Using this above query we can get the all employee list for each department, and then for each Decade and then for Male and Female.
here I have added for Sales department, for deacde "1991-2000" with Male gender, by changing these values for department, decade and gender.




2. 
 //below is pig script used to calculate avgerage Salary for male in decade  of 1981 to 1990
employee_details = LOAD '/usr/sales_male_1981_1990/000000_0' USING PigStorage(',') as (dept_name:chararray,col3:chararray, gender:chararray, salary:int,from_date:chararray);
employee_group_all = Group employee_details All;
employee_sal_avg = foreach employee_group_all  Generate
  AVG(employee_details.salary);
STORE employee_sal_avg INTO 'file:///home/vishnu/Documents/sales_male_1981_1990_Avg_csv';

Using this pig script calculated average salary for male employees under the decade of 1981 to 1990.
In same for other department, decade and gender we can get the result.

In this way,Each depatment then 4 derived decades  for Gender M and Gender F.
 For now submited only one departments with 2 decades with gender M and F as 4 files using the script.
 
 Now to get the remaning departments-decades-gender wise file need to chnage the values in script.
 
 due to in-suffecient space on created hadoop machine Not able to process this large number of all departmnets data, but bty changing values in given script will get the reqired otput files.
 
 ** Added Folder File details as:
 
 1. "Dept_sales_Gender_female_Deacde_1981_1990_Avg_csv" this folder, the "Part-r-00000" file get generated and also the "_SUCCESS" get generated and this  used  to store Average salary of all employees who belong under Sales department, Female gender and under 1981-1990 decade.
 
 
 2. "Dept_sales_Gender_female_Deacde_1991_2000" this folder have the CSV file  "000000_0" file  which have the List of Sales department , all female employees which belong under "1991-2000" decade.
 
 3."Dept_sales_Gender_female_Decade1981_1990" This folder have the CSV file "00000_0" file which have the list of Sales department , all female employees which belong under "1981-1990" decade.
 
 4."Dept_sales_Gender_male_Deacde_1981_1990" this folder have the CSV file "000000_0" file have the list of male employees of sales department under 1981-1990 decade.
 
 5. "Dept_sales_Gender_male_Deacde_1981_1990_Avg_csv" this folder have, the "Part-r-00000" file get generated and also the "_SUCCESS" get generated and this  used  to store Average salary of all employees who belong under Sales department, male gender and under 1981-1990 decade.
 
 6."Dept_sales_Gendermale_Decade1991_2000" this folder have the CSV file  "000000_0" file  which have the List of Sales department , all male employees which belong under "1991-2000" decade.
 
 7."Q2_Process_Scripts_used" this text file I have added or the process and script, query used to solve the question 2.
 
 
 
 
 
 
