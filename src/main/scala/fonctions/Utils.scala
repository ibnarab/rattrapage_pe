package fonctions

import org.apache.spark.sql.DataFrame
import Constants.spark

object Utils {


        def segmentationBaseTransactionOm(table: String, debut: String, fin: String): DataFrame = {

          spark.sql(
            s"""
              |SELECT TRIM(msisdn)            AS msisdn           ,
              |       TRIM(destinataire)      AS destinataire     ,
              |       TRIM(channel)           AS channel          ,
              |       TRIM(montant)           AS montant          ,
              |       TRIM(statuscode)        AS statuscode       ,
              |       TRIM(titre)             AS titre            ,
              |       TRIM(operation)         AS operation        ,
              |       TRIM(transactionid)     AS transactionid    ,
              |       TRIM(typetransaction)   AS typetransaction  ,
              |       TRIM(day)               AS day
              |
              |FROM $table
              |WHERE day
              |BETWEEN '$debut' AND '$fin'
            """.stripMargin)
        }


        def venteRebondAbonnement(table: String, year: String, month: String): DataFrame = {

          spark.sql(
            s"""
              |SELECT
              |       TRIM(num_ligne)         AS num_ligne      ,
              |       TRIM(code_techfam)      AS code_techfam   ,
              |       TRIM(mnt_abo)           AS mnt_abo        ,
              |       TRIM(type_offre)        AS type_offre     ,
              |       TRIM(mnt_taxe_fact)     AS mnt_taxe_fact  ,
              |       TRIM(year)              AS year           ,
              |       TRIM(month)             AS month
              |FROM $table
              |WHERE year = '$year' and month = '$month'
            """.stripMargin)

        }


        def venteRebondLocationNight(table1: String, table2: String, debut: String, fin: String): DataFrame = {

            val df1 = spark.sql(
              s"""
                |SELECT
                |       TRIM(msisdn)                        AS msisdn                   ,
                |       TRIM(region_30j)                    AS region                   ,
                |       TRIM(departement_30j)               AS departement              ,
                |       TRIM(commune_arrondissement_30j)    AS commune_arrondissement   ,
                |       TRIM(ca_cr_commune_30j)             AS ca_cr_commune            ,
                |       TRIM(year)                          AS year                     ,
                |       TRIM(month)                         AS month
                |FROM $table1
                |WHERE TRIM(day) = '$fin'
              """.stripMargin).na.drop(Seq("msisdn", "year", "month"))

          val df2 = spark.sql(
            s"""
              |SELECT
              |       TRIM(msisdn)                   AS msisdn      ,
              |       AVG(CAST(longitude AS double)) AS longitude   ,
              |       AVG(CAST(latitude  AS double)) AS latitude
              |FROM $table2
              |WHERE day
              |BETWEEN '$debut' AND '$fin'
              |GROUP BY msisdn
            """.stripMargin)

          df1.join(df2, Seq("msisdn"), "left").select(
            df1("msisdn")                     ,
            df1("region")                     ,
            df1("departement")                ,
            df1("commune_arrondissement")     ,
            df1("ca_cr_commune")              ,
            df2("longitude")                  ,
            df2("latitude")                   ,
            df1("year")                       ,
            df1("month")
          )
        }


        def venteRebondDossier(table: String): DataFrame = {
            spark.sql(
              s"""
                |SELECT
                |       TRIM(ncli)              AS ncli           ,
                |       TRIM(num_ligne)         AS num_ligne      ,
                |       TRIM(numero_contact)    AS num_contact    ,
                |       TRIM(day)               AS day
                |FROM $table
              """.stripMargin)
        }


        def venteRebondRechargeInDetail(table: String, debut: String, fin: String): DataFrame = {

            spark.sql(
              s"""
                |SELECT
                |       TRIM(msisdn)                                                AS msisdn   ,
                |       SUM(montant)                                                AS montant  ,
                |       YEAR(from_unixtime(unix_timestamp('$debut', 'yyyyMMdd')))   AS year     ,
                |       MONTH(from_unixtime(unix_timestamp('$debut', 'yyyyMMdd')))  AS month
                |FROM $table
                |WHERE day
                |BETWEEN '$debut' and '$fin'
                |GROUP BY msisdn, year, month
              """.stripMargin)
        }



        def terminaux(table: String, debut: String, fin: String): DataFrame = {
            spark.sql(
              s"""
                |SELECT
                |       TRIM(msisdn)                                                AS msisdn       ,
                |       TRIM(device)                                                AS device       ,
                |       smarphone                                                                   ,
                |       TRIM(compatible)                                            AS compatible   ,
                |       YEAR(from_unixtime(unix_timestamp('$debut', 'yyyyMMdd')))   AS year         ,
                |       MONTH(from_unixtime(unix_timestamp('$debut', 'yyyyMMdd')))  AS month
                |FROM $table
                |WHERE day
                |BETWEEN '$debut' AND '$fin'
                |GROUP BY msisdn, device, smarphone, compatible, year, month
              """.stripMargin)
        }
}
