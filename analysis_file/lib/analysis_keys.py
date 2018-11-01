import time

import pytz  # Timezone library for converting datetime objects between timezones
from core_data_modules.cleaners import Codes, somali
from core_data_modules.traced_data import Metadata
from dateutil.parser import isoparse


class AnalysisKeys(object):
    # TODO: Move some of these methods to Core Data?

    @staticmethod
    def get_date_time_utc(td):
        return isoparse(td["created_on"]).strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def get_date_time_eat(td):
        return isoparse(td["created_on"]).astimezone(pytz.timezone("Africa/Nairobi")).strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def set_yes_no_matrix_keys(user, td, show_keys, coded_shows_prefix, radio_q_prefix):
        matrix_d = dict()

        yes_no_key = coded_shows_prefix + "_yes_no"
        yes_no = td[yes_no_key]
        matrix_d[radio_q_prefix] = yes_no

        for key in td:
            if key.startswith(coded_shows_prefix) and key != yes_no_key:
                yes_prefix = radio_q_prefix + "_yes"
                no_prefix = radio_q_prefix + "_no"

                code_yes_key = key.replace(coded_shows_prefix, yes_prefix)
                code_no_key = key.replace(coded_shows_prefix, no_prefix)
                show_keys.update({code_yes_key, code_no_key})

                matrix_d[code_yes_key] = td[key] if yes_no == Codes.YES else "0"
                matrix_d[code_no_key] = td[key] if yes_no == Codes.NO else "0"

        td.append_data(matrix_d, Metadata(user, Metadata.get_call_location(), time.time()))

    @staticmethod
    def set_matrix_keys(user, td, show_keys, coded_shows_prefix, radio_q_prefix):
        matrix_d = dict()

        stopped = td.get("{}_{}".format(coded_shows_prefix, Codes.STOP)) == "1"

        for output_key in td:
            if output_key.startswith(coded_shows_prefix):
                code_key = output_key.replace(coded_shows_prefix, radio_q_prefix)

                if code_key.endswith(Codes.STOP):
                    continue

                show_keys.add(code_key)
                if stopped:
                    matrix_d[code_key] = Codes.STOP
                else:
                    matrix_d[code_key] = td[output_key]

        td.append_data(matrix_d, Metadata(user, Metadata.get_call_location(), time.time()))

    @classmethod
    def set_analysis_keys(cls, user, td, codes, scheme_id, code_id):
        #print(codes.get(td["Education_Coded"].get("SchemeID")).get(td["Education_Coded"]["CodeID"]))
        td.append_data({
            "UID": td["avf_phone_id"],

            #"gender": td["Gender_Coded"]["CodeID"],
            #"gender_raw": td["Gender (Text) - mcf_demog"],

            #"location": td["location_coded"]["CodeID"],
            #"location_raw": td["Location (Text) - mcf_demog"],
            
            "education": codes.get(scheme_id).get(code_id).display_text,
            "education_num": codes.get(scheme_id).get(code_id).numeric_value,
            #"education": codes[td["Education_Coded"]["CodeID"]].display_text,
            #"education_num": codes[td["Education_Coded"]["CodeID"]].numeric_value,
            "education_raw": td["Education (Text) - mcf_demog"]

            #"age": td["age_coded"]["CodeID"],
            #"age_raw": td["Age (Text) - mcf_demog"],

            #"work": codes[td["Work_Coded"]["CodeID"]].display_text,
            #"work_num":  codes[td["Work_Coded"]["CodeID"]].numeric_value,
            #"work_raw": td["Work (Text) - mcf_demog"]

            #"training": td["training_coded"]["CodeID"],
            #"training_raw": td["Training (Text) - mcf_demog"]
}, Metadata(user, Metadata.get_call_location(), time.time()))