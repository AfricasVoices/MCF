import argparse
import sys
import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO, TracedDataCSVIO
from core_data_modules.util.consent_utils import ConsentUtils
from core_data_modules.traced_data.util import FoldTracedData

from lib.analysis_keys import AnalysisKeys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates files for analysis from the cleaned and coded show "
                                                 "and survey responses")
    parser.add_argument("user", help="User launching this program")
    parser.add_argument("data_input_path", metavar="data-input-path",
                        help="Path to a coded JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to a JSON file to write serialized TracedData items to after modification by this"
                             "pipeline stage")
    parser.add_argument("csv_by_message_output_path", metavar="csv-by-message-output-path",
                        help="Analysis dataset where messages are the unit for analysis (i.e. one message per row)")
    parser.add_argument("csv_by_individual_output_path", metavar="csv-by-individual-output-path",
                        help="Analysis dataset where respondents are the unit for analysis (i.e. one respondent "
                             "per row, with all their messages joined into a single cell).")

    args = parser.parse_args()
    user = args.user
    data_input_path = args.data_input_path
    json_output_path = args.json_output_path
    csv_by_message_output_path = args.csv_by_message_output_path
    csv_by_individual_output_path = args.csv_by_individual_output_path

    # Serializer is currently overflowing
    # TODO: Investigate/address the cause of this.
    sys.setrecursionlimit(10000)

    demog_keys = [
        "location",
        "location_raw",
        "gender",
        "gender_raw",
        "age",
        "age_raw",
        "education",
        "education_raw",
        "work",
        "work_raw",
        "training",
        "training_raw"
    ]

    textit_consent_withdrawn_key = "mobilisation_consent_complete"
    avf_consent_withdrawn_key = "withdrawn_consent"
    source_flow_key = "source_flow"

    # Load cleaned and coded message/survey data
    with open(data_input_path, "r") as f:
        data = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    # Translate keys to final values for analysis
    show_keys = set()
    for td in data:
        AnalysisKeys.set_analysis_keys(user, td)
        """
        AnalysisKeys.set_matrix_keys(
            user, td, show_keys, "Employment_Idea (Text) - mcf_activation_coded",
            "work_opportunities"
        )
        """
    show_keys = list(show_keys)
    show_keys.sort()

    equal_keys = ["UID"]
    equal_keys.extend(demog_keys)
    concat_keys = ["employment_idea_raw"]
    matrix_keys = show_keys
    bool_keys = [
        avf_consent_withdrawn_key,

        #,
        #"bulk_sms",
        #"sms_ad",
        #"radio_promo",
        #"radio_show",
        #"non_logical_time"
    ]

    # Export to CSV
    export_keys = ["UID", "source_flow"]
    export_keys.extend(bool_keys)
    export_keys.extend(show_keys)
    export_keys.append("employment_idea_raw")
    export_keys.extend(demog_keys)

    # Set consent withdrawn based on presence of data coded as "stop"
    ConsentUtils.determine_consent_withdrawn(user, data, export_keys, avf_consent_withdrawn_key)

    # Set consent withdrawn based on auto-categorisation in Rapid Pro
    for td in data:
        if td.get(textit_consent_withdrawn_key) == "yes":  # Not using Codes.YES because this is from Rapid Pro
            td.append_data({avf_consent_withdrawn_key: Codes.TRUE}, Metadata(user, Metadata.get_call_location(), time.time()))

    for td in data:
        if avf_consent_withdrawn_key not in td:
            td.append_data({avf_consent_withdrawn_key: Codes.FALSE}, Metadata(user, Metadata.get_call_location(), time.time()))

    # Fold data to have one respondent per row
    to_be_folded = []
    for td in data:
        to_be_folded.append(td.copy())

    folded_data = FoldTracedData.fold_iterable_of_traced_data(
        user, data, fold_id_fn=lambda td: td["UID"],
        equal_keys=equal_keys, concat_keys=concat_keys, matrix_keys=matrix_keys, bool_keys=bool_keys
    )

    # Process consent
    stop_keys = set(export_keys) - {avf_consent_withdrawn_key}
    ConsentUtils.set_stopped(user, data, avf_consent_withdrawn_key)
    ConsentUtils.set_stopped(user, folded_data, avf_consent_withdrawn_key)

    # Output to CSV with one message per row
    with open(csv_by_message_output_path, "w") as f:
        TracedDataCSVIO.export_traced_data_iterable_to_csv(data, f, headers=export_keys)

    with open(csv_by_individual_output_path, "w") as f:
        TracedDataCSVIO.export_traced_data_iterable_to_csv(folded_data, f, headers=export_keys)

    # Export JSON
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(folded_data, f, pretty_print=True)