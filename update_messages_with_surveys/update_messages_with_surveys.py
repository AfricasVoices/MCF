import argparse

from core_data_modules.traced_data import TracedData
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Joins radio show answers with survey answers on respondents' "
                                                 "phone ids.")
    parser.add_argument("user", help="User launching this program")
    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to the input messages JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("feedback_json_input_path", metavar="feedback-json-input-path",
                        help="Path to the input messages JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("demog_survey_input_path", metavar="demog-survey-input-path",
                        help="Path to the demog JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("baseline_survey_input_path", metavar="baseline-survey-input-path",
                        help="Path to the baseline JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("event_date_survey_input_path", metavar="event-date-survey-input-path",
                        help="Path to the event date JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("event_time_survey_input_path", metavar="event-time-survey-input-path",
                        help="Path to the event time JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("event_name_survey_input_path", metavar="event-name-survey-input-path",
                        help="Path to the event name JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("speaker_question_survey_input_path", metavar="speaker-question-survey-input-path",
                        help="Path to the speaker question JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("endline_survey_input_path", metavar="endline-survey-input-path",
                        help="Path to the endline JSON file, containing a list of serialized TracedData objects")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to a JSON file to write processed messages to")

    args = parser.parse_args()
    user = args.user
    messages_json_input_path = args.messages_json_input_path
    feedback_json_input_path = args.feedback_json_input_path
    demog_survey_input_path = args.demog_survey_input_path
    baseline_survey_input_path = args.baseline_survey_input_path
    event_date_survey_input_path = args.event_date_survey_input_path
    event_time_survey_input_path = args.event_time_survey_input_path
    event_name_survey_input_path = args.event_name_survey_input_path
    speaker_question_survey_input_path = args.speaker_question_survey_input_path
    endline_survey_input_path = args.endline_survey_input_path
    json_output_path = args.json_output_path

    survey_datasets = list()

    # Load messages
    with open(messages_json_input_path, "r") as f:
        messages = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    # Load feedback messages
    with open(feedback_json_input_path, "r") as f:
        feedback = TracedDataJsonIO.import_json_to_traced_data_iterable(f)

    # Load demog surveys
    with open(demog_survey_input_path, "r") as f:
        demog_survey = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
        survey_datasets.append(demog_survey)

    # Load baseline surveys
    with open(baseline_survey_input_path, "r") as f:
        baseline_survey = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
        survey_datasets.append(baseline_survey)

    # Load event date survey
    with open(event_date_survey_input_path, "r") as f:
        event_date_survey = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
        survey_datasets.append(event_date_survey)

    # Load event time survey
    with open(event_time_survey_input_path, "r") as f:
        event_time_survey = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
        survey_datasets.append(event_time_survey)

    # Load event name survey
    with open(event_name_survey_input_path, "r") as f:
        event_name_survey = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
        survey_datasets.append(event_name_survey)

    # Load speaker question survey
    with open(speaker_question_survey_input_path, "r") as f:
        speaker_question_survey = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
        survey_datasets.append(speaker_question_survey)

    # Load endline survey
    with open(endline_survey_input_path, "r") as f:
        endline_survey = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
        survey_datasets.append(endline_survey)

    # Combine activation messages and feedback messages
    message_datasets = [messages, feedback]
    data = []
    for message_dataset in message_datasets:
        data.extend(message_dataset)

    # Add survey data to the messages
    for survey_dataset in survey_datasets:
        TracedData.update_iterable(user, "avf_phone_id", data, survey_dataset, "survey_responses")

    # Write json output
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(messages, f, pretty_print=True)
