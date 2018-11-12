import argparse
import os
import time
import random

import pytz
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO, TracedDataCodaIO, TracedDataCSVIO
from core_data_modules.util import IOUtils
from dateutil.parser import isoparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleans a list of messages, and outputs to formats "
                                                 "suitable for subsequent analysis")
    parser.add_argument("user", help="User launching this program")
    parser.add_argument("json_input_path", metavar="json-input-path",
                        help="Path to the input JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("prev_coda_path", metavar="prev-coda-path",
                        help="Path to a Coda file containing previously coded messages")
    parser.add_argument("flow_name", metavar="flow-name",
                        help="Name of activation flow from which this data was derived")
    parser.add_argument("variable_name", metavar="variable-name",
                        help="Name of message variable in flow")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to a JSON file to write processed messages to")
    parser.add_argument("coda_output_path", metavar="coda-output-path",
                        help="Path to a Coda file to write processed messages to")
    parser.add_argument("icr_output_path", metavar="icr-output-path",
                        help="Path to a CSV file to write 200 messages and run ids to, for use in inter-coder "
                             "reliability evaluation")
    parser.add_argument("csv_output_path",  metavar="csv-output-path",
                        help="Path to a CSV file to write all messages for thematic analysis")

    args = parser.parse_args()
    user = args.user
    json_input_path = args.json_input_path
    prev_coda_path = args.prev_coda_path
    variable_name = args.variable_name
    flow_name = args.flow_name
    json_output_path = args.json_output_path
    coda_output_path = args.coda_output_path
    icr_output_path = args.icr_output_path
    csv_output_path = args.csv_output_path
    
    ICR_MESSAGES_COUNT = 200  # Number of messages to export in the ICR file
    
    # Convert date/time of messages to EAT
    utc_key = "{} (Time) - {}".format(variable_name, flow_name)
    eat_key = "{} (Time EAT) - {}".format(variable_name, flow_name)
    inside_time_window = []
    START_TIME = isoparse("2018-10-18T00+03:00")
    END_TIME = isoparse("2018-10-27T00+03:00")

    # Load data from JSON file
    with open(json_input_path, "r") as f:
        show_messages = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    # Filter out test messages sent by AVF.
    show_messages = [td for td in show_messages if not td.get("test_run", False)]

    # Filter for runs which contain a response to this week's question.
    show_message_key = "{} (Text) - {}".format(variable_name, flow_name)
    show_messages = [td for td in show_messages if show_message_key in td]

    # Filter out messages sent outside the project run period
    for td in show_messages:
        utc_time = isoparse(td[utc_key])
        eat_time = utc_time.astimezone(pytz.timezone("Africa/Nairobi")).isoformat()

        td.append_data(
            {eat_key: eat_time},
            Metadata(user, Metadata.get_call_location(), time.time())
        )

        if START_TIME <= utc_time <= END_TIME:
            inside_time_window.append(td)
        else:
            print("Dropping: {}".format(utc_time))

    print("{}:{} Dropped as outside time/Total".format(len(show_messages) - len(inside_time_window), len(show_messages)))
    show_messages = inside_time_window

    # Output messages to a CSV file
    IOUtils.ensure_dirs_exist_for_file(csv_output_path)
    run_id_key = "{} (Run ID) - {}".format(variable_name, flow_name)
    raw_text_key = "{} (Text) - {}".format(variable_name, flow_name)
    with open(csv_output_path, "w") as f:
        TracedDataCSVIO.export_traced_data_iterable_to_csv(show_messages, f, headers=["avf_phone_id", run_id_key, raw_text_key])

    raw_text_key = "{} (Text) - {}".format(variable_name, flow_name)
    time_key = "{} (Time) - {}".format(variable_name, flow_name)

    # Update traced data objects with MessageID and Labels
    for td in show_messages:
        message_id = dict()
        labels = dict()
        hash_object = hashlib.sha256()
        hash_object.update(td[raw_text_key].encode('utf-8'))
        message_id_string = hash_object.hexdigest()
        message_id_key = "{} MessageID".format(raw_text_key)
        message_id[message_id_key] = message_id_string
        labels_key = "{} Labels".format(raw_text_key)
        labels[labels_key] = []
        td.append_data(message_id, Metadata(user, Metadata.get_call_location(), time.time()))
        td.append_data(labels, Metadata(user, Metadata.get_call_location(), time.time()))

    # Output messages to Coda
    IOUtils.ensure_dirs_exist_for_file(coda_output_path)
    message_ids = list()
    messages_to_code = list()
    for td in show_messages:
        output = dict()        
        output["Labels"] = td["{} Labels".format(plan.raw_field)]
        output["MessageID"] = td["{} MessageID".format(plan.raw_field)]
        output["Text"] = str(td[plan.raw_field])
        output["CreationDateTimeUTC"] = isoparse(td["{} (Time) - {}".format(plan.coda_name, "mcf_demog")]).isoformat()
        if output["MessageID"] not in message_ids:
                    messages_to_code.append(output)
                    message_ids.append(output["MessageID"])
        with open(coded_output_file_path, "w") as f:
            jsonpickle.set_encoder_options("json", sort_keys=True)
            f.write(jsonpickle.dumps(messages_to_code))
            f.write("\n")
            
    # Randomly select some messages to export for ICR
    random.seed(0)
    random.shuffle(show_messages)
    icr_messages = show_messages[:ICR_MESSAGES_COUNT]

    # Output ICR data to a CSV file
    run_id_key = "{} (Run ID) - {}".format(variable_name, flow_name)
    raw_text_key = "{} (Text) - {}".format(variable_name, flow_name)
    IOUtils.ensure_dirs_exist_for_file(icr_output_path)
    with open(icr_output_path, "w") as f:
        TracedDataCSVIO.export_traced_data_iterable_to_csv(icr_messages, f, headers=[run_id_key, raw_text_key])

    # Output to JSON
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(show_messages, f, pretty_print=True)
