import os
import time
import httpx
from sandboxer import Sandboxer

BASE_URL = os.environ.get("SANDBOXER_BASE_URL")
TOKEN = os.environ.get("SANDBOXER_TOKEN")

if not BASE_URL or not TOKEN:
    raise RuntimeError("❌ Missing SANDBOXER_BASE_URL or SANDBOXER_TOKEN env variable")

def main():
    sandboxer = Sandboxer(BASE_URL, TOKEN)

    # 🌐 Step 1: Create nginx sandbox
    sandbox = sandboxer.create(image="nginx", label="webserver", interactive=True)
    print(f"✅ Created nginx sandbox {sandbox.id}")

    # ✍️ Step 2: Write HTML to nginx index page
    html = "<html><body><h1>Hello from sandbox!</h1></body></html>"
    cmd = f"echo '{html}' > /usr/share/nginx/html/index.html"
    result = sandbox.exec(cmd)
    print("📝 Updated index.html")

    # ⏳ Step 3: Give nginx time to reload
    time.sleep(1)  # you can also poll or check logs if needed

    # 🌍 Step 4: Request the index page via the sandbox proxy
    # Assuming your server supports this route:
    proxy_url = f"{BASE_URL}/sandboxes/{sandbox.id}/proxy/"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    response = httpx.get(proxy_url, headers=headers)

    # ✅ Step 5: Check response
    assert response.status_code == 200, f"Expected 200 OK, got {response.status_code}"
    assert "Hello from sandbox!" in response.text, "HTML content not served as expected"
    print("🌐 Response OK:\n", response.text)

    # 🧹 Cleanup
    sandbox.terminate()
    print(f"🛑 Terminated sandbox {sandbox.id}")

if __name__ == "__main__":
    main()
