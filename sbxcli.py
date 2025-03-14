import requests
import json
import time

BASE_URL = "http://localhost:8000"

# Step 1: Create a root sandbox
create_response = requests.post(
    f"{BASE_URL}/sandboxes",
    json={"image": "nginx", "label": "Root Sandbox"},
)
if create_response.status_code != 200:
    print("Error creating sandbox:", create_response.text)
    exit(1)

root_sandbox = create_response.json()
root_sandbox_id = root_sandbox["id"]
print(f"✅ Created root sandbox: {root_sandbox_id} with label '{root_sandbox['label']}'")

# Step 2: Fork the root sandbox twice
fork_labels = ["First Fork", "Second Fork"]
forked_sandboxes = {}

for label in fork_labels:
    fork_response = requests.post(
        f"{BASE_URL}/sandboxes/{root_sandbox_id}/fork",
        json={"image": "nginx", "label": label},
    )
    if fork_response.status_code != 200:
        print(f"Error forking sandbox '{label}':", fork_response.text)
        exit(1)

    forked_sandbox = fork_response.json()
    forked_sandboxes[label] = forked_sandbox["id"]
    print(f"✅ Forked '{label}': {forked_sandbox['id']}")

# Step 3: Update labels for the forked sandboxes
updated_labels = {
    forked_sandboxes["First Fork"]: "Updated First Fork",
    forked_sandboxes["Second Fork"]: "Updated Second Fork"
}

for sbx_id, new_label in updated_labels.items():
    update_response = requests.patch(
        f"{BASE_URL}/sandboxes/{sbx_id}",
        json={"label": new_label},
    )
    if update_response.status_code != 200:
        print("Error updating label:", update_response.text)
        exit(1)
    print(f"✅ Updated sandbox {sbx_id} label to: {new_label}")

# Step 4: Fork "Updated Second Fork" again
grandchild_label = "Grandchild Fork"
second_fork_id = forked_sandboxes["Second Fork"]

grandchild_response = requests.post(
    f"{BASE_URL}/sandboxes/{second_fork_id}/fork",
    json={"image": "nginx", "label": grandchild_label},
)
if grandchild_response.status_code != 200:
    print("Error forking grandchild sandbox:", grandchild_response.text)
    exit(1)

grandchild_sandbox = grandchild_response.json()
grandchild_sandbox_id = grandchild_sandbox["id"]
print(f"✅ Forked grandchild sandbox: {grandchild_sandbox_id}")

# Step 5: Update the grandchild label
new_grandchild_label = "Updated Grandchild"
update_response = requests.patch(
    f"{BASE_URL}/sandboxes/{grandchild_sandbox_id}",
    json={"label": new_grandchild_label},
)
if update_response.status_code != 200:
    print("Error updating grandchild label:", update_response.text)
    exit(1)
print(f"✅ Updated grandchild label to: {new_grandchild_label}")

# Step 6: Print the sandbox tree from the root
tree_response = requests.get(f"{BASE_URL}/sandboxes/{root_sandbox_id}/tree")
if tree_response.status_code != 200:
    print("Error fetching sandbox tree:", tree_response.text)
    exit(1)

tree_data = tree_response.json()
print("\n🌲 Sandbox Tree (Formatted):")
print(tree_data["tree"])

print("\n🌲 Sandbox Tree (JSON):")
print(json.dumps(tree_data["tree_json"], indent=2))

# Step 7: Execute a simple command inside the root sandbox
exec_response = requests.post(
    f"{BASE_URL}/sandboxes/{root_sandbox_id}/execute",
    json={"code": "echo Hello World"},
)
if exec_response.status_code != 200:
    print("Error executing command:", exec_response.text)
    exit(1)

exec_result = exec_response.json()
print("\n🚀 Execution Result:")
print(f"STDOUT: {exec_result['stdout']}")
print(f"STDERR: {exec_result['stderr']}")
print(f"Exit Code: {exec_result['exit_code']}")

# Step 8: Stream a long-running command inside the root sandbox
print("\n🌀 Streaming Execution Output:")
stream_response = requests.post(
    f"{BASE_URL}/sandboxes/{root_sandbox_id}/execute?stream=true",
    json={"code": "bash -c 'for i in {1..5}; do echo Line $i; sleep 0.5; done'"},
    stream=True
)

if stream_response.status_code != 200:
    print("Error in streaming execution:", stream_response.text)
    exit(1)

for line in stream_response.iter_lines():
    if line:
        print(json.loads(line))  # Print each line as structured JSON

print("\n✅ Test completed successfully!")