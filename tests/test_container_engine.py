import asyncio
import time

from container_engine import ContainerEngine

async def main():
    engine = ContainerEngine()

    # 🌱 Step 1: Start the root container
    root = await engine.start_container(image='redis', name='root_sbx')
    print(f"✅ Started root sandbox {root.id} with IP {root.ip_address}")

    # 🌿 Step 2: Fork twice from the root container
    fork1, _ = await engine.fork_container(root.id, "fork_1")
    fork2, _ = await engine.fork_container(root.id, "fork_2")

    print(f"✅ Forked first sandbox {fork1.id}")
    print(f"✅ Forked second sandbox {fork2.id}")

    # 🍃 Step 3: Fork again from one of the forks
    deep_fork, _ = await engine.fork_container(fork2.id, "deep_fork")
    print(f"✅ Forked deep sandbox {deep_fork.id}")

    # 🛠 Step 4: Execute a command in the root container
    result, _, _ = await root.exec("redis-cli incr counter")
    print(f"🚀 Root Exec: INCR counter -> {result}")

    # 🛠 Step 5: Execute in fork1
    result, _, _ = await fork1.exec("redis-cli incr counter")
    print(f"🚀 Fork 1 Exec: INCR counter -> {result}")

    # 🛠 Step 6: Execute in fork2
    result, _, _ = await fork2.exec("redis-cli incr counter")
    print(f"🚀 Fork 2 Exec: INCR counter -> {result}")

    # 🛠 Step 7: Execute in deep fork
    result, _, _ = await deep_fork.exec("redis-cli incr counter")
    print(f"🚀 Deep Fork Exec: INCR counter -> {result}")

    # 🛠 Step 8: Verify data separation
    print("\n🔍 Verifying Data Separation:")
    for container, label in [(root, "Root"), (fork1, "Fork 1"), (fork2, "Fork 2"), (deep_fork, "Deep Fork")]:
        result, _, _ = await container.exec("redis-cli get counter")
        print(f"🗃 {label} counter -> {result}")

    # 🔄 Step 9: Streaming Execution
    print("\n🌀 Streaming Execution Output from Fork 2:")
    async for stream_type, data, exit_code in await fork2.exec("redis-cli get counter", stream=True):
        print(f"[{stream_type}] {data} (exit={exit_code})")

    # 🚮 Step 10: Cleanup
    for container in [deep_fork, fork2, fork1, root]:
        await container.stop()
        print(f"🛑 Stopped container {container.id}")

    await engine.close()
    print("\n✅ All containers stopped and cleaned up.")

asyncio.run(main())