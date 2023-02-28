import pandas as pd
from google.protobuf.message import Message

from typing import Any, Sequence


def proto_to_dict(proto_message: Message) -> dict[str, Any]:
    """
    Creates a dictionary from a given protobuf message while removing repeated fields

        Parameters:
            proto_message: A protobuf message

        Returns:
            proto_dict: A dictionary of the protobuf message
    """
    proto_dict = {}
    for field in proto_message.DESCRIPTOR.fields:
        if field.label == field.LABEL_REPEATED:
            continue
        if field.message_type:
            nested_dict = proto_to_dict(getattr(proto_message, field.name))
            proto_dict[field.name] = nested_dict
        else:
            proto_dict[field.name] = getattr(proto_message, field.name)
    return proto_dict


def proto_normalize(list_of_protos: Sequence[Message]) -> pd.DataFrame:
    """
    Create a DataFrame from a list of protobuf messages while removing repeated fields

        Parameters:
            list_of_protos: List of protobuf messages to be turned into a DataFrame

        Returns:
            df: A DataFrame containing protobuf information without repeated fields
    """
    protos_as_json = [proto_to_dict(proto) for proto in list_of_protos]
    df = pd.json_normalize(protos_as_json)
    return df.set_index(df.index.rename("index"))


def dicts_from_repeated_field(repeated_message, field, current_path, index_path):
    proto_list = []
    for i, message in enumerate(repeated_message):
        if field.message_type:
            proto_as_dict = proto_to_dict(message)
            proto_as_dict[index_path] = i
            proto_list.append(proto_as_dict)
        else:
            # primitive type
            proto_list.append({index_path: i, current_path: message})
    return proto_list


def explode_repeated(repeated_message, path_to_repeated_field, index_path):
    proto_list = []
    for i, message in enumerate(repeated_message):
            m = explode_field(message, path_to_repeated_field, index_path)
            for d in m:
                d[index_path] = i
            proto_list.extend(m)
    return proto_list


def explode_field(
    proto_message: Message, path_to_repeated_field: str, index_path: str
) -> list[dict[str, Any]]:
    """
    Creates a dictionary from a given protobuf message field. Recursively iterates
    through the nested protobuf messages and generates a dictionary from the last
    field in the path. An index is added to the dictionary for each nested
    message that is traversed.

        Parameters:
            proto_message: A protobuf message
            path_to_repeated_field: Relative path to field to be turned into dictionary
            index_path: Path to the current nested protobuf message

        Returns:
            proto_list: A list of dictionaries of the given repeated field
    """
    proto_list = []
    current_path, _, path_to_repeated_field = path_to_repeated_field.partition(".")
    index_path += "." + current_path
    nested_message = getattr(proto_message, current_path)
    field = proto_message.DESCRIPTOR.fields_by_name[current_path]
    if field.label == field.LABEL_REPEATED:
        if path_to_repeated_field == "":
            return dicts_from_repeated_field(
                nested_message, field, current_path, index_path
            )
        
        return explode_repeated(nested_message, path_to_repeated_field, index_path)
    elif field.message_type:
        return explode_field(nested_message, path_to_repeated_field, index_path)
    raise ValueError(
        f"Invalid path: {path_to_repeated_field}. {current_path} cannot be traversed."
    )


def proto_explode(
    list_of_protos: Sequence[Message], path_to_repeated_field: str
) -> pd.DataFrame:
    """
    Creates a pandas DataFrame from a list of protobuf messaged and a specified protobuf field.

        Parameters:
            list_of_protos: A list of protobuf messages
            path_to_repeated_field: a '.' separated path to the field to be turned into a DataFrame

        Returns:
            df: Pandas DataFrame
    """
    proto_list = []
    for i, proto in enumerate(list_of_protos):
        proto_as_dicts = explode_field(proto, path_to_repeated_field, "index")
        for d in proto_as_dicts:
            d["index."] = i
        proto_list.extend(proto_as_dicts)
    df = pd.json_normalize(proto_list)
    index_columns = sorted(index for index in df.columns if index.startswith("index."))
    df = df.set_index(index_columns)
    return df.set_index(df.index.rename({"index.": "index"}))
