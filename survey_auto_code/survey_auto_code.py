import argparse
import os
import time
from os import path
import hashlib

from core_data_modules.cleaners import swahili, Codes
from core_data_modules.traced_data import Metadata, TracedData
from core_data_modules.traced_data.io import TracedDataJsonIO, TracedDataCodaIO
from core_data_modules.util import IOUtils, PhoneNumberUuidTable


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleans the wt surveys and exports variables to Coda for "
                                                 "manual verification and coding")
    parser.add_argument("user", help="User launching this program, for use by TracedData Metadata")
    parser.add_argument("json_input_path", metavar="json-input-path",
                        help="Path to input file, containing a list of serialized TracedData objects as JSON")
    parser.add_argument("prev_coded_path", metavar="prev-coded-path",
                        help="Directory containing Coda files generated by a previous run of this pipeline stage. "
                             "New data will be appended to this file.")
    parser.add_argument("phone_uuid_table_path", metavar="phone-uuid-table-path",
                        help="JSON file containing an existing phone number <-> UUID lookup table.")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to a JSON file to write processed TracedData messages to")
    parser.add_argument("coded_output_path", metavar="coding-output-path",
                        help="Directory to write coding files to")

    args = parser.parse_args()
    user = args.user
    json_input_path = args.json_input_path
    prev_coded_path = args.prev_coded_path
    phone_uuid_table_path = args.phone_uuid_table_path
    json_output_path = args.json_output_path
    coded_output_path = args.coded_output_path

    class CleaningPlan:
        def __init__(self, raw_field, clean_field, coda_name, cleaner):
            self.raw_field = raw_field
            self.clean_field = clean_field
            self.coda_name = coda_name
            self.cleaner = cleaner

    cleaning_plan = [
        CleaningPlan("Gender (Text) - mcf_demog", "gender_clean", "Gender",
                     swahili.DemographicCleaner.clean_gender),
        CleaningPlan("Location (Text) - mcf_demog", "location_clean", "Location",
                     None),
        CleaningPlan("Education (Text) - mcf_demog", "education_clean", "Education",
                     None),
        CleaningPlan("Age (Text) - mcf_demog", "age_clean", "Age",
                     swahili.DemographicCleaner.clean_age),
        CleaningPlan("Work (Text) - mcf_demog", "work_clean", "Work",
                     None),
        CleaningPlan("Training (Text) - mcf_demog", "training_clean", "Training",
                     None),
    ]

    # Load phone number UUID table
    with open(phone_uuid_table_path, "r") as f:
        phone_uuids = PhoneNumberUuidTable.load(f)

    # Load data from JSON file
    with open(json_input_path, "r") as f:
        data = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    # Filter out test messages sent by AVF
    contacts = [td for td in data if not td.get("test_run", False)]

    # Mark missing entries in the raw data as true missing
    for td in data:
        missing = dict()
        for plan in cleaning_plan:
            if plan.raw_field not in td:
                missing[plan.raw_field] = Codes.TRUE_MISSING
        td.append_data(missing, Metadata(user, Metadata.get_call_location(), time.time()))

    # Clean all responses, add MessageID and Labels
    for td in data:
        cleaned = dict()
        message_id = dict()
        labels = dict()
        for plan in cleaning_plan:
            hash_object = hashlib.sha256()
            hash_object.update(td[plan.raw_field].encode('utf-8'))
            message_id_string = hash_object.hexdigest()
            message_id_key = "{} MessageID".format(plan.raw_field)
            message_id[message_id_key] = message_id_string
            labels_key = "{} Labels".format(plan.raw_field)
            labels[labels_key] = []
            if plan.cleaner is not None:
                cleaned[plan.clean_field] = plan.cleaner(td[plan.raw_field])
                labels[labels_key].append(cleaned[plan.clean_field])
        td.append_data(cleaned, Metadata(user, Metadata.get_call_location(), time.time()))
        td.append_data(message_id, Metadata(user, Metadata.get_call_location(), time.time()))
        td.append_data(labels, Metadata(user, Metadata.get_call_location(), time.time()))


    # Write json output
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(data, f, pretty_print=True)

    # TODO:remap message_id, labels, time to ingest by Coda v2
    keys = [run_id_key, raw_text_key, time_key]
    keymap = {run_id_key:"MessageID", raw_text_key:"Text", time_key:"CreationDateTimeUTC"}

    messages_to_code = dict("messages":[])
    for td in data:
        for plan in cleaning_plan:
                output = dict()
                output["Labels"] = td["{} Labels".format(plan.raw_field)]
                ouptput["MessageID"] = td["{} MessageID".format(plan.raw_field)]
                output["Text"] = td[plan.raw_field]
                #output["CreationDateTimeUTC"] = 

"""
    for td in data:
        for plan in cleaning_plan:

            td.append_data({"Labels":[]},  Metadata(user, Metadata.get_call_location(), time.time()))
            messages_to_code.append({key:td[key] for key in keys})

    for td in messages_to_code:
        td_remapped = {}
        for key in td:
            pass
            #td_remapped[keymap[key]] = td[key] 
"""

    # Output for manual verification + coding
    IOUtils.ensure_dirs_exist(coded_output_path)
    for plan in cleaning_plan:
        coded_output_file_path = path.join(coded_output_path, "{}.csv".format(plan.coda_name))
        prev_coded_output_file_path = path.join(prev_coded_path, "{}_coded.csv".format(plan.coda_name))

        if os.path.exists(prev_coded_output_file_path):
            with open(coded_output_file_path, "w") as f, open(prev_coded_output_file_path, "r") as prev_f:
                TracedDataCodaIO.export_traced_data_iterable_to_coda_with_scheme(
                    data, plan.raw_field, {plan.coda_name: plan.clean_field}, f, prev_f)
        else:
             with open(coded_output_file_path, "w") as f:
                 pass