package fonctions

import org.apache.spark.sql.SparkSession

object Constants {

      val spark = SparkSession.builder
        .appName("reconciliation")
        .config("spark.hadoop.fs.defaultFS", "hdfs://bigdata")
        .config("spark.master", "yarn")
        .config("spark.submit.deployMode", "cluster")
        .enableHiveSupport()
        .getOrCreate()


      val base_segmentation_om              =    "trusted_om.base_transaction_om"
      val base_abonnement                   =    "trusted_sicli.abonnement"
      val base_location_nightime            =    "refined_localisation.location_nightTime"
      val base_daily_clients                =    "refined_vue360.daily_clients"
      val base_dossier                      =    "trusted_sicli.dossier"
      val base_recharge_in_detail           =    "refined_recharge.recharge_in_detail"
      val base_terminaux                    =    "refined_trafic.terminaux"


      val write_transaction_om              =     "MYSVENTEREBONDPRD.segmentation_base_transaction_om"
      val write_rebond_abonnement           =     "MYSVENTEREBONDPRD.vente_rebond_abonnement"
      val write_rebond_location_night       =     "MYSVENTEREBONDPRD.vente_rebond_location_night"
      val write_rebond_dossier              =     "MYSVENTEREBONDPRD.vente_rebond_dossier"
      val write_rebond_recharge_in_detail   =     "MYSVENTEREBONDPRD.vente_rebond_recharge_in_detail"
      val write_terminaux                   =     "MYSVENTEREBONDPRD.terminaux"


      val mode_overwrite                    =     "overwrite"


}
