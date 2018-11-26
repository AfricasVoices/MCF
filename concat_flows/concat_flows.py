import argparse
import csv
import time
import re
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO


def normalise_activation_flow_keys(activation_td, key_to_normalise, normalised_key):
    """
    Normalises the keys from different PDMs(Post Distribuiton Monitoring survey)
    Acts on the TracedData Object itself(activation_td)
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
    parser.add_argument("radio_activation_input_path", help="Path to the radio activation survey")
    parser.add_argument("offline_activation_input_path", help="Path to the offline activation survey")
    parser.add_argument("traced_json_output_path", help="Path to concatenated TraceData JSON")
 
    args = parser.parse_args()
    user = args.user
    radio_activation_input_path = args.radio_activation_input_path
    offline_activation_input_path = args.offline_activation_input_path
    traced_json_output_path = args.traced_json_output_path

    # Load the 2 flows that were saved as JSON
    with open(radio_activation_input_path, 'r') as f:
        radio_activation_td_list = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
    with open(offline_activation_input_path, 'r') as f:
        offline_activation_td_list = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    # Set source flow (radio or offline)
    set_source_flow(radio_activation_td_list, 'radio')
    set_source_flow(offline_activation_td_list, 'offline')
    
    # Normalise the keys
    normalise_activation_flow_keys(radio_activation_td_list, 'mcf_activation_radio', 'mcf_activation')
    normalise_activation_flow_keys(offline_activation_td_list, 'mcf_activation_offline', 'mcf_activation')
    

    # Concatenate the flows
    activation_combined = []
    activation_combined.extend(radio_activation_td_list)
    activation_combined.extend(offline_activation_td_list)
    

    with open(traced_json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(activation_combined, f, pretty_print=True)