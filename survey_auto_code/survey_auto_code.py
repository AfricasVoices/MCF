import argparse
import os
import time
from os import path
import hashlib
from dateutil.parser import isoparse
import jsonpickle
import datetime
import json
import unicodecsv

from core_data_modules.cleaners import swahili, Codes
from core_data_modules.traced_data import Metadata, TracedData
from core_data_modules.traced_data.io import TracedDataJsonIO, TracedDataCSVIO
from core_data_modules.util import IOUtils, PhoneNumberUuidTable


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleans the surveys and exports variables to Coda for "
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
    parser.add_argument("coding_schemes_path", metavar="coding-schemes-path", help="Directory containing coding schemes")
    parser.add_argument("csv_output_path", metavar="csv-output-path", help="Directory to write thematic analysis csv files to")
    parser.add_argument("flow_name", metavar="flow-name")


    args = parser.parse_args()
    user = args.user
    json_input_path = args.json_input_path
    prev_coded_path = args.prev_coded_path
    phone_uuid_table_path = args.phone_uuid_table_path
    json_output_path = args.json_output_path
    coded_output_path = args.coded_output_path
    coding_schemes_path = args.coding_schemes_path
    csv_output_path = args.csv_output_path
    flow_name = args.flow_name

    CONTROL_CODES = ["NA", "NC", "WS"]

    class CleaningPlan:
        def __init__(self, raw_field, clean_field, coda_name, cleaner, scheme_id):
            self.raw_field = raw_field
            self.clean_field = clean_field
            self.coda_name = coda_name
            self.cleaner = cleaner
            self.scheme_id = scheme_id

    cleaning_plans = {"mcf_demog": [
        CleaningPlan("Gender (Text) - mcf_demog", "gender_clean", "Gender",
                     swahili.DemographicCleaner.clean_gender, "Scheme-12cb6f95"),
        CleaningPlan("Location (Text) - mcf_demog", "location_clean", "Location",
                     None, "Scheme-59ad3a2d3086"),
        CleaningPlan("Education (Text) - mcf_demog", "education_clean", "Education",
                     None, "Scheme-a57ce8d15245"),
        CleaningPlan("Age (Text) - mcf_demog", "age_clean", "Age",
                     swahili.DemographicCleaner.clean_age,
                    "Scheme-22b92dda5589"),
        CleaningPlan("Work (Text) - mcf_demog", "work_clean", "Work", None,
                     "Scheme-12be1d8f34eb"),
        CleaningPlan("Training (Text) - mcf_demog", "training_clean", "Training",
                     None, "Scheme-8f0794281bb1")],
                    
                    "event_date_poll":
        [CleaningPlan("Event_Date (Text) - event_date_poll", "event_date_clean", "Event_Date",
                     None, None)],
                    "event_name_poll":
        [CleaningPlan("Event_Name (Text) - event_name_poll", "event_name_clean", "Event_Name",
                     None, None)],
                     "event_time_poll":
        [CleaningPlan("Event_Time (Text) - event_time_poll", "event_time_clean", "Event_Time",
                     None, None)],
                     
                    "mcf_baseline":
        [CleaningPlan("Event_Expectation (Text) - mcf_baseline", "event_expectation_clean", "Event_Expectation",
                     None, None), 
        CleaningPlan("Dream_Work (Text) - mcf_baseline", "dream_work_clean", "Dream_Work",
                     None, None),
        CleaningPlan("Support_Yes_No (Text) - mcf_baseline", "support_yesno_clean", "Support_Yes_No",
                     None, None),
        CleaningPlan("Challenge (Text) - mcf_baseline", "challenge", "Challenge",
                     None, None),
        CleaningPlan("Voice_Projects_Likert (Text) - mcf_baseline", "voice_projects", "Voice_Projects_Likert",
                     None, None)]
                     }

    cleaning_plan = cleaning_plans[flow_name]

    # Load phone number UUID table
    with open(phone_uuid_table_path, "r") as f:
        phone_uuids = PhoneNumberUuidTable.load(f)

    # Load data from JSON file
    with open(json_input_path, "r") as f:
        data = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
    
    # Filter out test messages sent by AVF
    data = [td for td in data if not td.get("test_run", False)]

    # Mark missing entries in the raw data as true missing
    for td in data:
        missing = dict()
        for plan in cleaning_plan:
            if plan.raw_field not in td:
                missing[plan.raw_field] = Codes.TRUE_MISSING
        td.append_data(missing, Metadata(user, Metadata.get_call_location(), time.time()))

    # Exclude missing data
    for plan in cleaning_plan:
        data = [td for td in data if td[plan.raw_field] not in {Codes.TRUE_MISSING, Codes.SKIPPED, Codes.NOT_LOGICAL}]

    # Load code metadata from coding schemes
    code_ids = dict()
    IOUtils.ensure_dirs_exist(coding_schemes_path)
    for plan in cleaning_plan:
        if plan.scheme_id:
            print(plan.scheme_id)
            coding_scheme_path = path.join(coding_schemes_path, "{}.json".format(plan.coda_name))
            with open(coding_scheme_path, "r") as f:
                scheme = json.load(f)
                codes = scheme[0]["Codes"]
                code_ids[scheme[0]["SchemeID"]] = {}
                for code in codes:
                        if "ControlCode" in code:
                            code_text = code["ControlCode"]
                        else:
                            code_text = code["DisplayText"]
                        code_ids[scheme[0]["SchemeID"]][code_text] = code["CodeID"]            

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
                label = dict()
                cleaned[plan.clean_field] = str(plan.cleaner(td[plan.raw_field]))
                code_id = code_ids[plan.scheme_id][cleaned[plan.clean_field]]
                origin = {"OriginType":"Automatic","OriginID": "https://github.com/AfricasVoices/Project-MCF/pull/7", "Name": "survey_auto_code", "Metadata": {}}
                label["Checked"] = False
                label["Confidence"] = 0
                label["SchemeID"] = plan.scheme_id
                label["CodeID"] = code_id
                label["DateTimeUTC"] = datetime.datetime.utcnow().isoformat()
                label["Origin"] = origin
                labels[labels_key].append(label)
        td.append_data(cleaned, Metadata(user, Metadata.get_call_location(), time.time()))
        td.append_data(message_id, Metadata(user, Metadata.get_call_location(), time.time()))
        td.append_data(labels, Metadata(user, Metadata.get_call_location(), time.time()))

    # Write json output
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(data, f, pretty_print=True)
    
    # Output for manual verification + coding
    IOUtils.ensure_dirs_exist(coded_output_path)
    for plan in cleaning_plan:
        coded_output_file_path = path.join(coded_output_path, "{}.json".format(plan.coda_name))
        csv_output_file_path = path.join(csv_output_path, "{}.csv".format(plan.coda_name))
        message_ids = list()
        messages_to_code = list()
        avf_phone_ids = list()
        all_messages = list()
        for td in data:
                output = dict()        
                output["Labels"] = td["{} Labels".format(plan.raw_field)]
                output["MessageID"] = td["{} MessageID".format(plan.raw_field)]
                output["Text"] = str(td[plan.raw_field])
                output["CreationDateTimeUTC"] = isoparse(td["{} (Time) - {}".format(plan.coda_name, flow_name)]).isoformat()
                if output["MessageID"] not in message_ids:
                    messages_to_code.append(output)
                    message_ids.append(output["MessageID"])
                output["avf_phone_id"] = td["avf_phone_id"]
                if td["avf_phone_id"] not in avf_phone_ids:
                    all_messages.append(output)
                    avf_phone_ids.append(td["avf_phone_id"])
        with open(coded_output_file_path, "w") as f:
            jsonpickle.set_encoder_options("json", sort_keys=True)
            f.write(jsonpickle.dumps(messages_to_code))
            f.write("\n")
        with open(csv_output_file_path, "wb") as f:
            writer = unicodecsv.DictWriter(f, fieldnames=["avf_phone_id", "Text"], extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_messages)