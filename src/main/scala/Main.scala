object Main {

  import fonctions.Utils._
  import fonctions.Constants._
  import fonctions.ReadWrite._
  def main(args: Array[String]): Unit = {




        val debut = args(0)
        val fin   = args(1)
        val year  = args(2)
        val month = args(3)

        val df_segmentation_om            = segmentationBaseTransactionOm(base_segmentation_om, debut, fin)
        val df_rebond_abonnement          = venteRebondAbonnement(base_abonnement, year, month)
        val df_location_night             = venteRebondLocationNight(base_daily_clients, base_location_nightime, debut, fin)
        val df_rebond_abonnement_dossier  = venteRebondDossier(base_dossier)
        val df_recharge_in_detail         = venteRebondRechargeInDetail(base_recharge_in_detail, debut, fin)
        val df_terminaux                  = terminaux(base_terminaux, debut, fin)



        writeInMysql(df_segmentation_om             , mode_overwrite   , write_transaction_om)
        writeInMysql(df_rebond_abonnement           , mode_overwrite   , write_rebond_abonnement)
        writeInMysql(df_location_night              , mode_overwrite   , write_rebond_location_night)
        writeInMysql(df_rebond_abonnement_dossier   , mode_overwrite   , write_rebond_dossier)
        writeInMysql(df_recharge_in_detail          , mode_overwrite   , write_rebond_recharge_in_detail)
        writeInMysql(df_terminaux                   , mode_overwrite   , write_terminaux)


  }

}
