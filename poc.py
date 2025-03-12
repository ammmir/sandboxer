import asyncio
from jupyter_client.manager import AsyncKernelManager

async def main():
    # Connect to the Jupyter kernel
    km = AsyncKernelManager()
    await km.start_kernel()
    kc = km.client()
    kc.start_channels()
    await kc.wait_for_ready()

    # Execute the code
    msg_id = kc.execute("""
import matplotlib.pyplot as plt

print("hello")

fig, ax = plt.subplots()
ax.plot([1, 2])

plt.show()

print("world")
""")
    
    # Read execution results
    while True:
        reply = await kc.get_iopub_msg()
        if reply["parent_header"]["msg_id"] == msg_id:
            msg_type = reply["msg_type"]
            if msg_type == "stream":
                print(f'TEXT: {reply["content"]["text"]}')
            elif msg_type == "display_data":
                content = reply["content"]
                if "image/png" in content["data"]:
                    print(f'IMAGE: {content["data"]["image/png"]}\n')
            elif msg_type == "error":
                print(f'ERROR: {reply["content"]["traceback"][0]}')
                break
            elif msg_type == "status" and reply["content"]["execution_state"] == "idle":
                break
    
    # Cleanup
    kc.shutdown()
    
asyncio.run(main())
