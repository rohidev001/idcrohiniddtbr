# Databricks notebook source
dbutils.widgets.text("storageaccountname", "","")
dbutils.widgets.get("storageaccountname")
storageaccountname = getArgument("storageaccountname")

#test 


dbutils.widgets.text("sourcecontainername", "","")
dbutils.widgets.get("sourcecontainername")
sourcecontainername = getArgument("sourcecontainername")

dbutils.widgets.text("destinationcontainername", "","")
dbutils.widgets.get("destinationcontainername")
destinationcontainername = getArgument("destinationcontainername")

dbutils.widgets.text("accesskey", "","")
dbutils.widgets.get("accesskey")
accesskey = getArgument("accesskey")

dbutils.widgets.text("SAStoken", "","")
dbutils.widgets.get("SAStoken")
SAStoken = getArgument("SAStoken")

dbutils.widgets.text("sourcefolder", "","")
dbutils.widgets.get("sourcefolder")
sourcefolder = getArgument("sourcefolder")

dbutils.widgets.text("destinationfolder", "","")
dbutils.widgets.get("destinationfolder")
destinationfolder = getArgument("destinationfolder")

spark.conf.set("fs.azure.account.key."+storageaccountname+".blob.core.windows.net",
  accesskey)
  
spark.conf.set(
  "fs.azure.sas."+sourcecontainername+"."+storageaccountname+".blob.core.windows.net",SAStoken)

df = spark.read.format("csv").option("inferSchema","true").load("wasbs://"+sourcecontainername+"@"+storageaccountname+".blob.core.windows.net/"+sourcefolder+"/")
#df.show()

df.write.csv("wasbs://"+destinationcontainername+"@"+storageaccountname+".blob.core.windows.net/"+destinationfolder+"/")
