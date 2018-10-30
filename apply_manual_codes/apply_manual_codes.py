import argparse
import time
from os import path
import json
from time import isoparse

from core_data_modules.cleaners import CharacterCleaner, Codes
from core_data_modules.cleaners.codes import SomaliaCodes
from core_data_modules.cleaners.location_tools import SomaliaLocations
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO, TracedDataCodaIO, TracedDataTheInterfaceIO
from core_data_modules.util import IOUtils


# TODO: remove once pull request for below merged with master in CoreDataModules
class TracedDataCoda2IO(object):
    @classmethod
    def import_coda_to_traced_data_iterable(cls, user, data, data_message_id_key, scheme_keys, f):
        """
        Codes a "column" of a collection of TracedData objects by using the codes from a Coda data-file.
        Data which is has not been assigned a code in the Coda file is coded using the NR code from the provided scheme.
        :param user: Identifier of user running this program.
        :type user: str
        :param data: TracedData objects to be coded using the Coda file.
        :type data: iterable of TracedData
        :param data_message_id_key: Key in TracedData objects of the message ids.
        :type data_message_id_key: str
        :param scheme_keys: Dictionary of of the key in each TracedData object of coded data for a scheme to
                            a Coda 2 scheme object.
        :type scheme_keys: dict of str -> list of dict
        :param f: Coda data file to import codes from.
        :type f: file-like
        """
        # Build a lookup table of MessageID -> SchemeID -> Labels
        coda_dataset = dict()  # of MessageID -> (dict of SchemeID -> list of Label)
        for msg in json.load(f):
            schemes = dict()  # of SchemeID -> list of Label
            coda_dataset[msg["MessageID"]] = schemes
            msg["Labels"].reverse()
            for label in msg["Labels"]:
                scheme_id = label["SchemeID"]
                if scheme_id not in schemes:
                    schemes[scheme_id] = []
                schemes[scheme_id].append(label)

        # Apply the labels from Coda to each TracedData item in data
        for td in data:
            for key_of_coded, scheme in scheme_keys.items():
                labels = coda_dataset.get(td[data_message_id_key], dict()).get(scheme["SchemeID"])
                if labels is None:
                    not_reviewed_code_id = \
                        [code["CodeID"] for code in scheme["Codes"] if code["CodeID"].startswith("code-NR")][0]
                    td.append_data(
                        {key_of_coded: {
                            "CodeID": not_reviewed_code_id,
                            "SchemeID": scheme["SchemeID"]
                            # TODO: Set the other keys which label would have had here had they come from Coda?
                        }},
                        Metadata(user, Metadata.get_call_location(), time.time())
                    )
                else:
                    for label in labels:
                        td.append_data(
                            {key_of_coded: label},
                            Metadata(label["Origin"]["OriginID"], Metadata.get_call_location(),
                                     isoparse(label["DateTimeUTC"]).timestamp())
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merges manually cleaned files back into a traced data file.")
    parser.add_argument("user", help="User launching this program, for use by TracedData Metadata")
    parser.add_argument("json_input_path", metavar="json-input-path",
                        help="Path to JSON input file, which contains a list of TracedData objects")
    parser.add_argument("coded_input_path", metavar="coded-input-path",
                        help="Directory to read manually-coded Coda files from")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to a JSON file to write merged results to")
    parser.add_argument("scheme_input_path", metavar="scheme-input-path",
                        help="Directory to read Coda scheme files from")
                        
    
    args = parser.parse_args()
    user = args.user
    json_input_path = args.json_input_path
    coded_input_path = args.coded_input_path
    json_output_path = args.json_output_path
    interface_output_dir = args.interface_output_dir

    class MergePlan:
        def __init__(self, coda_name, coded_name):
            self.coda_name = coda_name
            self.coded_name = coded_name

    merge_plan = [
        MergePlan("Gender", "Gender_Coded"),
        MergePlan("Location", "Location_Coded"),
        MergePlan("Education", "Education_Coded"),
        MergePlan("Training", "Training_Coded"),
        MergePlan("Work", "Work_Coded"),
        MergePlan("Age", "Age_Coded"),
    ]

    # Load data from JSON file
    with open(json_input_path, "r") as f:
        data = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    # Merge manually coded survey Coda files into the cleaned dataset
    for plan in merge_plan:
        coda_file_path = path.join(coded_input_path, "{}_coded.json".format(plan.coda_name))

        if not path.exists(coda_file_path):
            print("Warning: No Coda file found for key '{}'".format(plan.coda_name))
            for td in data:
                td.append_data(
                    {plan.coded_name: None},
                    Metadata(user, Metadata.get_call_location(), time.time())
                )
            continue

        scheme_file_path = path.join(scheme_file_path, "{}.json").format(plan.coda_name)
        with open(scheme_file_path, "r") as f:
            coding_scheme = json.load(f)

        with open(coda_file_path, "r") as f:
            TracedDataCoda2IO.import_coda_to_traced_data_iterable(
                user, data, "MessageID", {plan.coded_name: coding_scheme}, f)

    # Write coded data back out to disk
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(data, f, pretty_print=True)

    