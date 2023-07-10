# Execute a notebook over Jupyter Lab API
#
# Based on https://stackoverflow.com/a/54551221/942520

import os
import sys
import argparse
import json
import requests
import datetime
import uuid
import argparse
import base64
from pprint import pprint
from websocket import create_connection, WebSocketTimeoutException

if "JUPYTER_TOKEN" not in os.environ:
    raise Exception("JUPYTER_TOKEN environment variable must be set")


def parse_args(event):
    # Create an argument parser
    parser = argparse.ArgumentParser()

    # Add positional arguments
    parser.add_argument(
        "jupyter_server",
        nargs="?",
        default=os.getenv("JUPYTER_SERVER"),
        help="Jupyter server address and port",
    )
    parser.add_argument(
        "jupyter_notebook",
        nargs="?",
        default=os.getenv("JUPYTER_NOTEBOOK"),
        help="Notebook path on Jupyter server with a leading slash",
    )
    parser.add_argument(
        "--use-https",
        action="store_true",
        default=False,
        help="Use https instead of http",
    )
    parser.add_argument(
        "--verbose", action="store_true", default=False, help="Print debug messages"
    )

    # Parse the command line arguments or use the event payload
    if event is not None and "data" in event:
        pprint(event["data"])
        data = pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        args = parser.parse_args(
            data.split()
        )  # Split the payload into a list of arguments
    else:
        args = parser.parse_args()

    # Print help if no arguments are provided
    if args.jupyter_server is None or args.jupyter_notebook is None:
        parser.print_help()
        sys.exit(1)

    return args


def send_execute_request(code):
    msg_type = "execute_request"
    content = {"code": code, "silent": False}
    hdr = {
        "msg_id": uuid.uuid1().hex,
        "username": "test",
        "session": uuid.uuid1().hex,
        "data": datetime.datetime.now().isoformat(),
        "msg_type": msg_type,
        "version": "5.0",
    }
    msg = {"header": hdr, "parent_header": hdr, "metadata": {}, "content": content}
    return msg


# Main function
# This is the entrypoint for both local and cloud execution
# The event and context arguments are only used when running in the cloud
def main(event, context):
    args = parse_args(event)

    # Notebook path on Jupyter server with a leading slash
    notebook_path = args.jupyter_notebook
    # Either http or https
    protocol = "https" if args.use_https else "http"
    # Jupyter server address and port
    jupyter_server = args.jupyter_server
    headers = {"Authorization": f"Token {os.environ['JUPYTER_TOKEN']}"}

    # Create a kernel
    url = f"{protocol}://{jupyter_server}/api/kernels"
    print(f"Creating a new kernel at {url}", file=sys.stderr)
    with requests.post(url, headers=headers) as response:
        pprint({"kernel": response.headers}, sys.stderr) if args.verbose else None
        kernel = json.loads(response.text)

    # Load the notebook and get the code of each cell
    url = f"{protocol}://{jupyter_server}/api/contents{notebook_path}"
    with requests.get(url, headers=headers) as response:
        file = json.loads(response.text)
    pprint({"notebook_content": file}, sys.stderr) if args.verbose else None
    # filter out non-code cells like markdown
    code = [
        c["source"]
        for c in file["content"]["cells"]
        if len(c["source"]) > 0 and c["cell_type"] == "code"
    ]

    # Execution request/reply is done on websockets channels
    ws = create_connection(
        f"{'ws' if protocol == 'http' else 'wss'}://{jupyter_server}/api/kernels/{kernel['id']}/channels",
        header=headers,
    )

    print("Sending execution requests for each cell", file=sys.stderr)
    for c in code:
        ws.send(json.dumps(send_execute_request(c)))

    code_blocks_to_execute = len(code)

    while code_blocks_to_execute > 0:
        try:
            rsp = json.loads(ws.recv())
            msg_type = rsp["msg_type"]
            if msg_type == "error":
                pprint({"exception": rsp["content"]}, sys.stderr)
                raise Exception(rsp["content"]["traceback"][0])
        except Exception as _e:
            print(_e, sys.stderr)
            break

        if msg_type == "execute_result" or args.verbose:
            pprint(rsp["content"])

        if (
            msg_type == "execute_reply"
            and rsp["metadata"].get("status") == "ok"
            and rsp["metadata"].get("dependencies_met", False)
        ):
            code_blocks_to_execute -= 1

    print("Processing finished. Closing websocket connection", file=sys.stderr)
    ws.close()

    # Delete the kernel
    print("Deleting kernel", file=sys.stderr)
    url = f"{protocol}://{jupyter_server}/api/kernels/{kernel['id']}"
    response = requests.delete(url, headers=headers)


if __name__ == "__main__":
    main(None, None)
