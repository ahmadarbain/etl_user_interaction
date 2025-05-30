def get_value_from_dict(dict_object: dict, dict_key: str, default_value: any) -> any:
    """
    Get value from dictionary object.

    :param dict_object: dict: dictionary object.
    :param dict_key: str: dictionary key.
    :param default_value: any: default value if `dict_key` doesn't exists.
    :return: any: the value of the dictionary object.
    """
    if dict_key in dict_object:
        return dict_object[dict_key]
    else:
        return default_value
