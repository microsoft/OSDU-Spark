# Spark Datasource Connector for Open Subsurface Data Universe (OSDU)


## Features
* Surface OSDU records as Spark data frames
* Filtering by type and/or kind
* Translation of OSDU schema to Spark schema
  * Primitive types (string, integer, bool,...)
  * Arrays
  * Objects
* Column pruning: based on selected fields on the Spark side, only the requested columns are requested and transferred by the OSDU instance


Brief Example
```scala
    val sampleDf = sc.read
        .format("com.microsoft.spark.osdu")
        .option("kind", "osdu:wks:master-data--GeoPoliticalEntity:1.0.0")
        .option("query", "")
        .option("osduApiEndpoint", "<INSERT>")
        .option("partitionId", "<INSERT>")
        .option("bearerToken","<INSERT>")
        .load

    sampleDf.printSchema()

    sampleDf.show()
```


Output

    root
    |-- data: struct (nullable = true)
    |    |-- ParentGeoPoliticalEntityID: string (nullable = true)
    |    |-- GeoPoliticalEntityTypeID: string (nullable = true)
    |    |-- TerminationDate: string (nullable = true)
    |    |-- DisputedIndicator: boolean (nullable = true)
    |    |-- DaylightSavingTimeStartDate: string (nullable = true)
    |    |-- GeoPoliticalEntityName: string (nullable = true)
    |    |-- GeoPoliticalEntityID: string (nullable = true)
    |    |-- DaylightSavingTimeEndDate: string (nullable = true)
    |    |-- EffectiveDate: string (nullable = true)
    |-- kind: string (nullable = false)
    |-- version: integer (nullable = true)
    |-- modifyUser: string (nullable = true)
    |-- modifyTime: string (nullable = true)
    |-- createTime: string (nullable = true)
    |-- status: string (nullable = true)
    |-- createUser: string (nullable = true)
    |-- id: string (nullable = true)

    +--------------------+--------------------+----------+----------+----------+--------------------+------+--------------------+--------------------+
    |                data|                kind|   version|modifyUser|modifyTime|          createTime|status|          createUser|                  id|
    +--------------------+--------------------+----------+----------+----------+--------------------+------+--------------------+--------------------+
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    |[,,,,, United Sta...|osdu:wks:master-d...|2147483647|      null|      null|2022-01-18T17:18:...|  null|2f59abbc-7b40-4d0...|osdukmtest-opendes...|
    +--------------------+--------------------+----------+----------+----------+--------------------+------+--------------------+--------------------+

## Dev
To run the sandbox run 

```bash
cd spark/datasource
sbt testOnly 
```


## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
