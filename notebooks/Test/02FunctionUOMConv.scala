// Databricks notebook source
val adlsMountPath               = "/mnt"; 
val processedParquetMastersPath = "/00custom/02staging/01ebs/parquet/01masters/"; 
val adlsPath                    = adlsMountPath + processedParquetMastersPath;  
val dfUOMConv = spark.read.parquet(adlsPath + "MtlUomConversions"); 
dfUOMConv.createOrReplaceTempView("mtl_uom_conversions_view");

val dfUOM = spark.read.parquet(adlsPath + "MtlUnitsOfMeasure"); 
dfUOM.createOrReplaceTempView("MTL_UNITS_OF_MEASURE");  

val dfClassConv = spark.read.parquet(adlsPath + "MtlUomClassConversions"); 
dfClassConv.createOrReplaceTempView("MTL_UOM_CLASS_CONVERSIONS");  
dfUOMConv.unpersist; 
dfUOM.unpersist;
dfClassConv.unpersist;

val ITEM = spark.sql(""" SELECT DISTINCT T1.INVENTORY_ITEM_ID,  
                                         T1.UOM_CODE FROM_UOM,
                                         T2.UOM_CODE TO_UOM   
                         FROM mtl_uom_conversions_view T1   
                              FULL OUTER JOIN 
                              mtl_uom_conversions_view T2
                              ON T1.INVENTORY_ITEM_ID=T2.INVENTORY_ITEM_ID  """); 

ITEM.createOrReplaceTempView("ITEM"); 
ITEM.unpersist(); 
val UOMCLASS = spark.sql(""" SELECT DISTINCT T1.UOM_CODE, T1.UOM_CLASS FROM_CLASS, T2.UOM_CLASS TO_CLASS   FROM       MTL_UNITS_OF_MEASURE T1   FULL OUTER JOIN       MTL_UNITS_OF_MEASURE T2   ON T1.UOM_CODE=T2.UOM_CODE   """); UOMCLASS.createOrReplaceTempView("UOMCLASS"); UOMCLASS.unpersist();  val ITEMS_1 = spark.sql("""SELECT DISTINCT T1.INVENTORY_ITEM_ID,  T1.FROM_UOM, T1.TO_UOM, T2.FROM_CLASS, T3.TO_CLASS FROM ITEM T1 LEFT OUTER JOIN UOMCLASS T2  	ON T1.FROM_UOM=T2.UOM_CODE LEFT OUTER JOIN UOMCLASS T3  	ON T1.TO_UOM=T3.UOM_CODE """); 
