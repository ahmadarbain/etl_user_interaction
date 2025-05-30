import re


def remove_trailing_char(char: str, text: str) -> str:
    """Remove trailing characters from string

    Args:
        char (str): Chars to be removed
        text (str): Text string

    Returns:
        str: Processed text string
    """
    return re.sub(char+'$', '', text)


def snake_to_camel(text: str) -> str:
    """Convert snake case to upper camel case.

    Args:
        text (str): Snake case text

    Returns:
        str: Generated upper camel case
    """
    return ''.join(x.capitalize() or '_' for x in text.split('_'))


def str_to_snake(text: str) -> str:
    """Convert unformatted string to snake case.

    Args:
        text (str): Unformatted string

    Returns:
        str: Generated snake case
    """
    return re.sub('[^0-9a-zA-Z]+', '_', text).rstrip('_')


def remove_parentheses(text: str) -> str:
    """Remove parentheses from string.

    Args:
        text (str): Unformatted string

    Returns:
        str: The string with removed parentheses
    """
    return re.sub(r"\([^()]*\)", "", text)
