import httpx
import json
import time

BASE_URL = "http://localhost:8000"

# Step 1: Create a root sandbox
create_response = httpx.post(
    f"{BASE_URL}/sandboxes",
    json={"image": "jmalloc/echo-server", "label": "Root Sandbox"},
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
    fork_response = httpx.post(
        f"{BASE_URL}/sandboxes/{root_sandbox_id}/fork",
        json={"label": label},
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
    update_response = httpx.patch(
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

grandchild_response = httpx.post(
    f"{BASE_URL}/sandboxes/{second_fork_id}/fork",
    json={"label": grandchild_label},
)
if grandchild_response.status_code != 200:
    print("Error forking grandchild sandbox:", grandchild_response.text)
    exit(1)

grandchild_sandbox = grandchild_response.json()
grandchild_sandbox_id = grandchild_sandbox["id"]
print(f"✅ Forked grandchild sandbox: {grandchild_sandbox_id}")

# Step 5: Update the grandchild label
new_grandchild_label = "Updated Grandchild"
update_response = httpx.patch(
    f"{BASE_URL}/sandboxes/{grandchild_sandbox_id}",
    json={"label": new_grandchild_label},
)
if update_response.status_code != 200:
    print("Error updating grandchild label:", update_response.text)
    exit(1)
print(f"✅ Updated grandchild label to: {new_grandchild_label}")

# Step 6: Print the sandbox tree from the root
tree_response = httpx.get(f"{BASE_URL}/sandboxes/{root_sandbox_id}/tree")
if tree_response.status_code != 200:
    print("Error fetching sandbox tree:", tree_response.text)
    exit(1)

tree_data = tree_response.json()
print("\n🌲 Sandbox Tree (Formatted):")
print(tree_data["tree"])

print("\n🌲 Sandbox Tree (JSON):")
print(json.dumps(tree_data["tree_json"], indent=2))

# Step 7: Execute a simple command inside the root sandbox
exec_response = httpx.post(
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
with httpx.stream("POST", f"{BASE_URL}/sandboxes/{root_sandbox_id}/execute?stream=true",
    json={"code": "sh -c 'for i in {1..5}; do echo Line $i; sleep 0.5; done'"}) as r:
    if r.status_code != 200:
        print("Error in streaming execution:", stream_response.text)
        exit(1)
    for line in r.iter_lines():
        if line:
            print(json.loads(line))  # Print each line as structured JSON

# ✅ Step 9: Perform a streaming SSE GET request inside the sandbox
print("\n🌍 Testing streaming SSE GET request inside the sandbox:")
with httpx.stream("GET", f"{BASE_URL}/sandboxes/{root_sandbox_id}/proxy/.sse") as r:
    print(f"✅ GET Response Status: {r.status_code}")
    i = 0
    for line in r.iter_raw():
        i += 1
        if line:
            print(line)
        if i > 5:
            break

# ✅ Step 10: Perform a POST request inside the sandbox
print("\n🌍 Testing POST request inside the sandbox:")
post_data = {"key": "value"}
post_response = httpx.post(f"{BASE_URL}/sandboxes/{root_sandbox_id}/proxy/foo", json=post_data)

if post_response.status_code != 200:
    print("Error making POST request:", post_response.text)

print(f"✅ POST Response Status: {post_response.status_code}")
print("✅ POST Response Body:")
print(post_response.text)

# ✅ Step 11: Delete all sandboxes (grandchild first, then second fork, then first fork, then root)
print("\n🗑️ Deleting sandboxes...")

def delete_sandbox(sandbox_id):
    delete_response = httpx.delete(f"{BASE_URL}/sandboxes/{sandbox_id}")
    if delete_response.status_code != 200:
        print(f"Error deleting sandbox {sandbox_id}:", delete_response.text)
    print(f"🗑️ Deleted sandbox {sandbox_id}")

# Delete in bottom-up order
delete_sandbox(grandchild_sandbox_id)
delete_sandbox(forked_sandboxes["Second Fork"])
delete_sandbox(forked_sandboxes["First Fork"])
delete_sandbox(root_sandbox_id)

print("\n✅ Test completed successfully!")