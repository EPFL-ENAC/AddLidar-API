import re
import json
from html import escape

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
        all_arguments.append({
            "flag": f"-{arg}",
            "description": desc
        })
    
    for arg, desc in long_arguments:
        if arg not in ["help", "version"]:  # Skip common flags
            all_arguments.append({
                "flag": f"--{arg}",
                "description": desc if desc else "No description"
            })
    
    # Create result dictionary
    result = {
        "error_message": error_message,
        "command": "./lidarDataManager",
        "usage": command_syntax,
        "arguments": all_arguments,
        "help_text": "For complete USAGE and HELP type: ./lidarDataManager --help"
    }
    
    return result

def to_json(parsed_data):
    return json.dumps(parsed_data, indent=2)

def to_html(parsed_data):
    html = """<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .error { color: red; font-weight: bold; margin-bottom: 20px; }
        .command { font-family: monospace; margin-bottom: 20px; background-color: #f5f5f5; padding: 10px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .help { font-style: italic; margin-top: 20px; }
    </style>
    <title>CLI Error</title>
</head>
<body>
    <div class="error">""" + escape(parsed_data["error_message"]) + """</div>
    
    <h2>Command</h2>
    <div class="command">""" + escape(parsed_data["command"]) + """</div>
    
    <h2>Usage</h2>
    <div class="command">""" + escape(parsed_data["usage"]) + """</div>
    
    <h2>Arguments</h2>
    <table>
        <tr>
            <th>Flag</th>
            <th>Description</th>
        </tr>"""
    
    for arg in parsed_data["arguments"]:
        html += f"""
        <tr>
            <td>{escape(arg["flag"])}</td>
            <td>{escape(arg["description"])}</td>
        </tr>"""
    
    html += """
    </table>
    
    <div class="help">""" + escape(parsed_data["help_text"]) + """</div>
</body>
</html>"""
    
    return html