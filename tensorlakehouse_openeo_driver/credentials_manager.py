#!/usr/bin/env python
import json
import base64
from pathlib import Path
from typing import Any, Dict
import click


def base64_encode(credential_mapping: Dict[str, Any]) -> str:
    # Convert the credential mapping to JSON format
    credential_mapping_json = json.dumps(credential_mapping)

    # Encode the JSON data as bytes
    encoded_bytes = credential_mapping_json.encode("utf-8")

    # Encode the bytes using Base64
    base64_encoded = base64.b64encode(encoded_bytes).decode("utf-8")

    return base64_encoded


def read_credentials(path: Path):
    with open(path, "r") as f:
        credentials = json.load(f)
        return credentials


def encode_credentials(credentials: Dict[str, Any]) -> str:
    base64_encoded = base64_encode(credential_mapping=credentials)
    return base64_encoded
    # save_file(base64_encoded=base64_encoded)


def decode_credential(encoded_credentials: str) -> Dict[str, Any]:
    """decode credentials which had been encoded using base64

    https://stackabuse.com/encoding-and-decoding-base64-strings-in-python/

    Args:
        encoded_credentials (str): encoded credentials

    Returns:
        Dict[str, Any]: credentials as dict
    """
    # Decode the Base64-encoded data
    decoded_bytes = base64.b64decode(encoded_credentials)

    # Convert the decoded bytes to a string (assuming it was originally a JSON string)
    decoded_string = decoded_bytes.decode("utf-8")

    # Parse the decoded string into a data structure (assuming it was originally a JSON object)
    decoded_data = json.loads(decoded_string)
    assert isinstance(decoded_data, dict)
    return decoded_data


@click.command()
@click.option(
    "--file",
    prompt="full path to the file that contains the credentials",
    help="full path to the Json file that contains all credentials",
)
def generate(file):
    input_file = Path(file)

    assert input_file.exists(), f"Error! File does not exist: {input_file}"
    generate_base64_credentials(input_file)


def generate_base64_credentials(credentials: Path):
    with open(credentials, "r") as f:
        cred = json.load(f)
        encoded = encode_credentials(credentials=cred)
        print("New CREDENTIALS env variable below:")
        print(encoded)

        decoded = decode_credential(encoded_credentials=encoded)
        # print("*" * 50)
        # print(decoded)
        # print("*" * 50)
        assert cred == decoded, f"{cred}\n{decoded}"


if __name__ == "__main__":
    generate()
