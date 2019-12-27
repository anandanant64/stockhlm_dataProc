# stockhlm_dataProc

A repository for stockhlm temperature and pressure data processing. Bringing the data from various url files to hdfs cluster keeping the 
integrity same. 
Creating one table for temperature and another for pressure readings.

DATABASE : stockhlm
TABLE : temperature, pressure

#Usage

create jar and run in hdfs cluster using below local commnd :

    1)  In local mode :
    spark-submit --class "stockhlm.data.process.ProcessStockhlm" --master local[*]  <jar File Path>/<jar_name.jar>

OR  2) pull in as existing project/maven to eclipse/IntelliJ and run stockhlm.stockhlm_dataProc.processStockhlm scala object.

#Results

Database - "stockhlm" with two tables - "temperature, pressure" will be created which will be of Internal. Both the tables can be used for 
any of the research works. Given some directory locations and Database Name, external table can also be created with ease.
