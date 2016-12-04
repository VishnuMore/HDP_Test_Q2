# HDP_Test_Q2
Output data of hadoop test question 2


# Q2:
Find out Salary trends between Male and Female employees during each decade in every department. The output should be in the following format: Department, Decade, Gender, avg_salary. Please upload the final CSV along with the code and jar files(if any) to your github and provide the link below.

# Result:

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




# 2. //below is pig script used to calculate avgerage Salary for male in decade  of 1991 to 2000

""
employee_details = LOAD '/home/vishnu/documents/sales_male_1991_2000/000000_0' USING PigStorage(',') as (dept_name:chararray,col3:chararray, gender:chararray, salary:int,from_date:chararray); employee_group_all = Group employee_details All; employee_sal_avg = foreach employee_group_all Generate flatten(employee_details),AVG(employee_details.salary); STORE employee_sal_avg INTO 'file:///home/vishnu/Desktop/sales_male_1991_2000_Avg_csv';

""

Using this pig script calculated average salary for male employees under the decade of 1991 to 2000.
In same for other department, decade and gender we can get the result.

In this way,Each depatment then 4 derived decades  for Gender M and Gender F.
 For now submited only one departments with 2 decades with gender M and F as 4 files using the script.
 
 Now to get the remaning departments-decades-gender wise file need to chnage the values in script.
 
 due to in-suffecient space on created hadoop machine Not able to process this large number of all departmnets data, but bty changing values in given script will get the reqired otput files.
 
 
 # The Other Hive and pig commnds used to generate the HIVE table CSV and Out put file in PIG as:
 
 # Following HIVE query is used to get the male from sales department in the decade of 1981-1990
INSERT OVERWRITE LOCAL DIRECTORY '/home/vishnu/Documents/sales_male_1981_1990' row format delimited fields terminated by ',' select d.dept_name,'1991-2000' as col3,e.gender,s.salary,de.from_date from employees e left join salaries s on (s.emp_no = e.emp_no) left join dept_emp1 de on (e.emp_no = de.emp_no) left join departments d on(d.dept_no = de.dept_no) where (de.from_date between 1981 AND 1990 OR de.to_date between 1981 AND 1990) AND e.gender LIKE 'M' AND d.dept_name LIKE '%Sales%' order by de.from_date;

# Following PIG script is used to get the desired output. It takes csv file as input which is genrated by HIVE
employee_details = LOAD '/home/vishnu/Documents/sales_male_1981_1990/000000_0' USING PigStorage(',') as (dept_name:chararray,col3:chararray, gender:chararray, salary:int,from_date:chararray);
employee_group_all = Group employee_details All;
employee_sal_avg = foreach employee_group_all  Generate
  flatten(employee_details),AVG(employee_details.salary);
STORE employee_sal_avg INTO 'file:///home/vishnu/Desktop/sales_male_1981_1990_Avg_csv';

# Following HIVE query is used to get the females from sales department in the decade of 1981-1990
INSERT OVERWRITE LOCAL DIRECTORY '/home/vishnu/Documents/sales_female_1981_1990' row format delimited fields terminated by ',' select d.dept_name,'1991-2000' as col3,e.gender,s.salary,de.from_date from employees e left join salaries s on (s.emp_no = e.emp_no) left join dept_emp1 de on (e.emp_no = de.emp_no) left join departments d on(d.dept_no = de.dept_no) where (de.from_date between 1981 AND 1990 OR de.to_date between 1981 AND 1990) AND e.gender LIKE 'F' AND d.dept_name LIKE '%Sales%' order by de.from_date;

# Following PIG script is used to get the desired output. It takes csv file as input which is genrated by HIVE
employee_details = LOAD '/home/vishnu/Documents/sales_female_1981_1990/000000_0' USING PigStorage(',') as (dept_name:chararray,col3:chararray, gender:chararray, salary:int,from_date:chararray);
employee_group_all = Group employee_details All;
employee_sal_avg = foreach employee_group_all  Generate
  flatten(employee_details),AVG(employee_details.salary);
STORE employee_sal_avg INTO 'file:///home/vishnu/Desktop/sales_female_1981_1990_Avg_csv';


# Following HIVE query is used to get the females from sales department in the decade of 1991-2000
INSERT OVERWRITE LOCAL DIRECTORY '/home/vishnu/Documents/sales_female_1991_2000' row format delimited fields terminated by ',' select d.dept_name,'1991-2000' as col3,e.gender,s.salary,de.from_date from employees e left join salaries s on (s.emp_no = e.emp_no) left join dept_emp1 de on (e.emp_no = de.emp_no) left join departments d on(d.dept_no = de.dept_no) where (de.from_date between 1991 AND 2000 OR de.to_date between 1991 AND 2000) AND e.gender LIKE 'F' AND d.dept_name LIKE '%Sales%' order by de.from_date;

# Following PIG script is used to get the desired output. It takes csv file as input which is genrated by HIVE
employee_details = LOAD '/home/vishnu/Documents/sales_female_1991_2000/000000_0' USING PigStorage(',') as (dept_name:chararray,col3:chararray, gender:chararray, salary:int,from_date:chararray);
employee_group_all = Group employee_details All;
employee_sal_avg = foreach employee_group_all  Generate
  flatten(employee_details),AVG(employee_details.salary);
STORE employee_sal_avg INTO 'file:///home/vishnu/Desktop/sales_female_1991_2000_Avg_csv';
 
 
 
 #  Added Folder File details as:
 
 Here I have added main Zip "Q2_OUTPUT_DATA &Details.tar.zip" which contains below parts:
 
 1."csv_genrated_using_hive_for_sales_1981_1990_M_F" this directory contains the "Sales" department , Male and female output data of 2 Decades CSV files of tables generated using HIVE. :- 
  1.1 1981-1990 (male and Female)
  1.2 1991-2000 (male and female)
  
 2."Second_question_output_csv_for_1981_1990_Male_Female" this directory contains output CSV for Q2 using the PIG:
 
  2.1 1981-1990 (male and Female)
  2.2 1991-2000 (male and female)
 
 
 
 
 
