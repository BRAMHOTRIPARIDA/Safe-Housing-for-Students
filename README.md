Safe Housing for Students
-----------------------------------------
Project Objective - Develop a data model using NLP that identifies safe zones in areas enclosing USC using its Public Safety online reports to help students take informed decisions about housing.

Dataset : USC Department of Public Safety online reports - https://dps.usc.edu/category/alerts/ 

Tasks and Methodology :
-----------------------------------------
Gathered DPS alerts datasets from website mentioned above.

Developed code for web scrapping the DPS Alerts data.

Cleaned, pre-processed and transformed the raw unordered data into a usable data format (with attributes such as incident alert link, incident type, description, date/time of occurence, location, vehicle and suspect description) to extract the unique locations per incident and the frequency of DPS alerts related to these locations using NLP.

Enhanced the extraction efficiency of the NLP algorithm to 90% by implementing customized functions.

Incorporated code to write this DPS alerts location-frequency data back to Amazon AWS RDS MySQL database. 

Developed a Python scheduler to update the data in regular intervals on EC2 so that the database is always up to date. 
