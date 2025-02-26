import re
import json


def parse_cli_error(error_text):
    # Extract the main components
    parts = error_text.split("\n\n")

    # Get error message from first part
    error_message = parts[0].strip()

    # Get usage info
    usage_match = re.search(r"Brief USAGE:\s*\n(.*?)(?:\n\n|\Z)", error_text, re.DOTALL)
    usage = usage_match.group(1).strip() if usage_match else ""

    # Extract command syntax
    command_syntax = ""
    if usage:
        command_lines = [line.strip() for line in usage.split("\n")]
        command_syntax = " ".join(command_lines)

    # Extract arguments
    arg_pattern = r"-([a-zA-Z])=\\u003C([^>]*)\\u003E"
    arguments = re.findall(arg_pattern, error_text)

    # Extract long arguments
    long_arg_pattern = r"--([a-zA-Z_]+)(?:=\\u003C([^>]*)\\u003E)?"
    long_arguments = re.findall(long_arg_pattern, error_text)

    # Combine all arguments
    all_arguments = []
    for arg, desc in arguments:
        all_arguments.append({"flag": f"-{arg}", "description": desc})

    for arg, desc in long_arguments:
        if arg not in ["help", "version"]:  # Skip common flags
            all_arguments.append(
                {"flag": f"--{arg}", "description": desc if desc else "No description"}
            )

    # Create result dictionary
    result = {
        "error_message": error_message,
        "command": "./lidarDataManager",
        "usage": command_syntax,
        "arguments": all_arguments,
        "help_text": "For complete USAGE and HELP type: ./lidarDataManager --help",
    }

    return result


def to_json(parsed_data):
    return json.dumps(parsed_data, indent=2)
