from ingestion.ingest_recareas import run as ingest_recareas
from ingestion.ingest_facilities import run as ingest_facilities
from ingestion.ingest_campsites import run as ingest_campsites

from transformation.clean_recareas import run as clean_recareas
from transformation.clean_facilities import run as clean_facilities
from transformation.clean_campsites import run as clean_campsites
from transformation.clean_campsite_attributes import run as clean_campsite_attributes
from transformation.clean_campsite_equipment import run as clean_campsite_equipment

from transformation.gold_campsites_per_facility import run as gold_campsites_per_facility
from transformation.gold_campsites_per_recarea import run as gold_campsites_per_recarea
from transformation.gold_rv_sites import run as gold_rv_sites


def run_pipeline():

    print("===== INGESTION =====")

    ingest_recareas()
    ingest_facilities()
    ingest_campsites()

    print("===== SILVER TRANSFORM =====")

    clean_recareas()
    clean_facilities()
    clean_campsites()
    clean_campsite_attributes()
    clean_campsite_equipment()

    print("===== GOLD LAYER =====")

    gold_campsites_per_facility()
    gold_campsites_per_recarea()
    gold_rv_sites()

    print("===== PIPELINE COMPLETE =====")


if __name__ == "__main__":
    run_pipeline()
