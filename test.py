import os
import requests
import json

TOKEN = os.getenv("TFC_TOKEN")
ORG   = "your-org-name"
WS    = "my-awesome-infra"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/vnd.api+json"
}

# 1. Get workspace ID
r = requests.get(f"https://app.terraform.io/api/v2/organizations/{ORG}/workspaces/{WS}", headers=HEADERS)
ws_id = r.json()["data"]["id"]

# 2. Get current state version → contains download URL
r = requests.get(f"https://app.terraform.io/api/v2/workspaces/{ws_id}/current-state-version", headers=HEADERS)
download_url = r.json()["data"]["attributes"]["hosted-state-download-url"]

# 3. Download the state (no auth needed on the signed URL)
state_response = requests.get(download_url)
state = json.loads(state_response.content)

print(json.dumps(state, indent=2))

# Save
with open(f"terraform.tfstate.{WS}", "w", encoding="utf-8") as f:
    json.dump(state, f, indent=2)
--------------------------------------------
from terrasnek.api import TFC
import os
import json

TOKEN = os.getenv("TFC_TOKEN")
ORG   = "your-org-name"
WS    = "my-awesome-infra"

api = TFC(TOKEN, url="https://app.terraform.io")
api.set_org(ORG)

ws = api.workspaces.show(WS)["data"]
ws_id = ws["id"]

current_state = api.state_versions.current(ws_id)["data"]
download_url = current_state["attributes"]["hosted-state-download-url"]

# Download raw state (returns bytes)
state_bytes = api.state_versions.download(download_url)

state = json.loads(state_bytes)
print(json.dumps(state, indent=2))

# Save to file
with open(f"terraform.tfstate.{WS}", "w", encoding="utf-8") as f:
    json.dump(state, f, indent=2)
