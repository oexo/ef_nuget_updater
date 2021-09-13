from datetime import datetime
import json
from pathlib import Path
import os
import requests

from packaging.version import parse
import jsonschema
from jsonschema import validate

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))

with open(Path(MAIN_DIR + "/" + "./config.json"), "r") as main_config:
    MAIN_CONFIG = json.load(main_config)

with open(Path(MAIN_DIR + "/" + MAIN_CONFIG["dh_api_scheme"]), "r") as dh_schema:
    DH_SCHEMA = json.load(dh_schema)

DOWNLOADED_MODELS = Path(MAIN_DIR + "/" + MAIN_CONFIG["file_path_for_save_download_history"])
END_FOLDER = Path(MAIN_CONFIG["folder_for_package_save"])


def is_json_valid(json_data: dict, json_schema: dict) -> bool:
    """
    Func for validate json by the python module jsonschema
    :param json_data: Dictionary for confirmation
    :param json_schema: Confirmation scheme
    :return: Bool, The True if the dict is valid, and False if not.
    """
    try:
        validate(instance=json_data, schema=json_schema)
    except jsonschema.exceptions.ValidationError as err:
        return False
    return True


def determine_senior_version(model_versions: dict) -> str:
    """
    Check dict value and determine most latest version
    :param model_versions: Dictionary with numbers of version as value.
        for example: "{'dh1': '1.0.166.1', 'dh2': '1.0.198.0', 'addin': '1.0.198.0'}"
    :return: String with most latest version.
        for example: "1.0.198.0"
    """
    senior_ver = parse("0.0")
    for ver in model_versions.values():
        if senior_ver < parse(ver):
            senior_ver = parse(ver)
    return str(senior_ver)


def new_create_log_message(incident_name: str, **kwargs) -> str:
    """
    The func create log message from vars
    :param incident_name: The name and the code of incident. For example: "Error 1"
    :param kwargs: Other optional vars, such as "url_name", "url_path" etc.
    :return: The formated message for print or write to file.
    """
    incident_type, incident_code = incident_name.split()
    url_name_list = kwargs["url_name_list"] if "url_name_list" in kwargs else None
    url_name = kwargs["url_name"].lower() if "url_name" in kwargs else None
    url_path = kwargs["url_path"].lower() if "url_path" in kwargs else None

    incidents = {
        "Info": [
            "JSON was decode",
            f"Package was download from URL: { url_path }"
        ],
        "Warning": [
            "JSON is not valid",
            f"JSON did not loaded from URL: { url_path }"
        ],
        "Error": [
            f"No version was found in { url_name_list }",
            f"Package download error from URL: { url_path }"
        ],
        "Disaster": [
            "No one package was downloaded"
        ]
    }
    yield f"{ datetime.now() } -- { incident_type } \t { url_name }:\t { incidents[incident_type][int(incident_code)] }"


def print_log_message(error_msg):
    """
    Print the messages from list
    :param error_msg: List with messages
    """
    for msg in error_msg:
        print(msg)


def get_mv_from_urls(dict_with_urls: dict, dict_with_json_schema: dict, searched_field: str):
    """
    Taking URLSs from URL_CONFIG and getting the "modelsVersion" from the response
    :param dict_with_urls: Dict with a url-name as a key and a url-path as a value.
        for example: {
                         "urls_with_model_version": {
                             "dh1": "https://dh1.efir-net.ru/v2/system/ping",
                             "dh2": "https://dh2.efir-net.ru/v2/system/ping2",
                             "addin": "https://addin.efir-net.ru/v2/system/ping2"
                         }
                      }
    :param dict_with_json_schema: Dict for the python module jsonschema, for validate loading from url json.
        for example: {
                         "type": "object",
                         "required": ["modelsVersion"],
                         "properties": {
                             "modelsVersion": {
                                  "type": "string",
                                   "pattern": "1\\A(\\d\\.){2}.*\\Z"

                             }
                         }
                     }
    :param seached_field: The key of the value containing the version number.
        for example: "modelsVersion"
    :return: Dictionary with model versions.
        for example: "{'dh1': '1.0.166.1', 'dh2': '1.0.198.0', 'addin': '1.0.198.0'}"
    """
    model_versions = {}
    url_name_list = []

    for url_name, url_path in dict_with_urls["urls_with_model_version"].items():
        url_name_list.append(url_name)
        try:
            with requests.get(url_path) as api_response:
                parsed = json.loads(api_response.text)
                if is_json_valid(parsed, dict_with_json_schema):
                    model_versions[url_name] = delete_closing_zero(parsed[searched_field])
                    incident = "Info 0"
                else:
                    incident = "Warning 0"
        except json.decoder.JSONDecodeError:
            incident = "Warning 1"
        print_log_message(new_create_log_message(incident, url_name=url_name, url_path=url_path))
    return model_versions, url_name_list


def download_nuurls_with_model_package(dict_with_urls: dict, dh_model_version: str) -> int:
    """
    The func downloads Nuget package from URLs
    :param dict_with_urls:  Dict with a url-name as a key and a url-path as a value.
        for example: {
                           "urls_with_model_package": {
                               "prgt": "http://ef-proget.devel.ifx/nuget/Datahub/package/Efir.DataHub.Models/"
                           }
                      }
    :param dh_model_version: String with model version
    """
    return_code = 0
    for url_name, url_path in dict_with_urls["urls_with_model_package"].items():
        url_path = add_closing_slash(url_path) + dh_model_version
        try:
            with requests.get(url_path, stream=True) as r:
                r.raise_for_status()
                local_filename = Path(str(END_FOLDER) + "/" + r.headers["Content-Disposition"].split("=")[1].strip('"'))
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            incident = "Info 1"
        except requests.exceptions.HTTPError:
            incident = "Error 1"
            return_code = 1
        print_log_message(new_create_log_message(incident_name=incident, url_name=url_name, url_path=url_path))
    return return_code


def delete_closing_zero(model_version: str) -> str:
    """
    Delete the closing zero in model version string
    :param model_version: for example: 1.0.166.0
    :return: for example: 1.0.166
    """
    if model_version[-2:] == ".0":
        return model_version[:-2]
    return model_version


def add_closing_slash(url_path: str) -> str:
    """
    Adding a closing slash if it doesn't find in the string
    :param url_path: for example: http://ef-proget.devel.ifx/nuget/Datahub/package/Efir.DataHub.Models
    :return: for example: http://ef-proget.devel.ifx/nuget/Datahub/package/Efir.DataHub.Models/
    """
    if url_path[-1:] != "/":
        url_path = url_path + "/"
    return url_path


def has_model_already_been_downloaded(downloaded_models_file: Path, model_version: str) -> bool:
    """
    Finding model version in list with already downloaded version
    :param downloaded_models_file: The path of file with the list
    :param model_version: The version of the model to be found in the list
    :return: Bool: True if the version of the model already was downloaded and False if not
    """
    string_pattern = f"{ model_version } \n"

    with open(downloaded_models_file, "r") as models_r:
        return string_pattern in models_r


def update_downloaded_mv_in_file(downloaded_models_file: Path, model_version: str):
    """
    Write the version of model which been downloaded to the file
    :param downloaded_models_file: The path of file with the list
    :param model_version: The version of the model to be write to the list
    """
    string_pattern = f"{ model_version } \n"

    with open(downloaded_models_file, "a+") as models_w:
        models_w.write(string_pattern)


def create_empty_file(filename: Path):
    with open(filename, "w"):
        pass


def main():
    searched_field = "modelsVersion"
    models, url_name_list = get_mv_from_urls(MAIN_CONFIG, DH_SCHEMA, searched_field)

    if models:
        dh_model_version = determine_senior_version(models)

        if not os.path.exists(DOWNLOADED_MODELS):
            create_empty_file(Path(DOWNLOADED_MODELS))

        if not has_model_already_been_downloaded(DOWNLOADED_MODELS, dh_model_version):
            download_code = download_nuurls_with_model_package(MAIN_CONFIG, dh_model_version)
            if download_code == 0:
                update_downloaded_mv_in_file(DOWNLOADED_MODELS, dh_model_version)
    else:
        print_log_message(new_create_log_message("Error 0", url_name_list=url_name_list))
        print_log_message(new_create_log_message("Disaster 0"))


if __name__ == "__main__":
    main()
