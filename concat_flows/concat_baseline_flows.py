import argparse
import csv
import time
import re
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO


def normalise_activation_flow_keys(activation_td, key_to_normalise, normalised_key):
    """
    Normalises the keys from different baseline surveys
    Acts on the TracedData Object itself
    :param activation_td: Data to normalise the keys of
    :type activation_td: iterable of TracedData
    :param key_to_normalise: Key to search for and normalise
    :type key_to_normalise: str
    :param normalised_key: String create new key from 
    :type normalised_key: str
    """
    for record in activation_td:
        data_to_append = {}
        for key in record.keys():
            if key_to_normalise in key:
                new_key = re.sub(key_to_normalise, normalised_key, key)
                data_to_append[new_key] = record[key]
        md =  Metadata(user, Metadata.get_call_location(), time.time())
        record.append_data(data_to_append, md)


def set_source_flow(activation_td, source_flow):
    for record in activation_td:
        source_flow_data = {"source_flow": source_flow}
        record.append_data(source_flow_data, Metadata(user, Metadata.get_call_location(), time.time()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Loads the activation surveys and concatenates them")
    parser.add_argument("user", help="Identifier of user launching this program, for use in TracedData Metadata")
    parser.add_argument("baseline_input_path", help="Path to the radio activation survey")
    parser.add_argument("baseline_reminder_1_input_path", help="Path to the offline activation survey")
    parser.add_argument("baseline_reminder_2_input_path", help="Path to the offline activation survey")
    parser.add_argument("traced_json_output_path", help="Path to concatenated TraceData JSON")
 
    args = parser.parse_args()
    user = args.user
    baseline_input_path = args.baseline_input_path
    baseline_reminder_1_input_path = args.baseline_reminder_1_input_path
    baseline_reminder_2_input_path = args.baseline_reminder_2_input_path
    traced_json_output_path = args.traced_json_output_path

    # Load the 3 flows that were saved as JSON
    with open(baseline_input_path, 'r') as f:
        baseline = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
    with open(baseline_reminder_1_input_path, 'r') as f:
        baseline_reminder_1 = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
    with open(baseline_reminder_2_input_path, 'r') as f:
        baseline_reminder_2 = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    baseline_reminder_1 = [td for td in baseline_reminder_1 if not td.get("Baseline_Complete_Split (Category) - mcf_baseline_reminder", "Completed")]
    baseline_reminder_2 = [td for td in baseline_reminder_2 if not td.get("Baseline_Complete_Split (Category) - mcf_baseline_reminder_2", "Completed")]

    # Normalise the keys
    normalise_activation_flow_keys(baseline_reminder_1, 'mcf_baseline_reminder_1', 'mcf_baseline')
    normalise_activation_flow_keys(baseline_reminder_2, 'mcf_baseline_reminder_2', 'mcf_baseline')
    
    # Concatenate the flows
    baseline_combined = []
    baseline_combined.extend(baseline)
    baseline_combined.extend(baseline_reminder_1)
    baseline_combined.extend(baseline_reminder_2)

    with open(traced_json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(baseline_combined, f, pretty_print=True)