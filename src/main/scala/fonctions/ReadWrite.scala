package fonctions

import org.apache.spark.sql.DataFrame

object ReadWrite {

  def writeInMysql(dataFrame: DataFrame, mode: String, table: String): Unit = {

    dataFrame
      .write
      .format("jdbc")
      .mode(mode)
      .partitionBy("year", "month")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://10.137.15.6:3306/MYSVENTEREBONDPRD")
      .option("dbtable", table)
      .option("user", "u_vente_rebond")
      .option("password", "S0n@telV3nteReb0nd2023")
      //.option("batchsize", jdbcBatchSize)
      .save()
  }

}
