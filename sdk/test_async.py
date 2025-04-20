import asyncio
import os
from sandboxer import Sandboxer

BASE_URL = os.environ.get("SANDBOXER_BASE_URL")
TOKEN = os.environ.get("SANDBOXER_TOKEN")

if not BASE_URL or not TOKEN:
    raise RuntimeError("❌ Missing SANDBOXER_BASE_URL or SANDBOXER_TOKEN env variable")

async def main():
    sandboxer = Sandboxer(BASE_URL, TOKEN)

    # 🌱 Step 1: Create root sandbox
    root = await sandboxer.acreate(image="redis", label="root", interactive=False)
    print(f"✅ Started root sandbox {root.id}")

    # Step 2: Increment counter in root
    await root.aexec("redis-cli incr counter")
    print("🚀 Root: INCR counter")

    # 🌿 Step 3: Fork → fork1
    fork1 = await root.afork(label="fork_1")
    print(f"✅ Forked 1 -> {fork1.id}")

    # Step 4: Increment counter in fork1
    await fork1.aexec("redis-cli incr counter")
    print("🚀 Fork 1: INCR counter")

    # 🍃 Step 5: Fork → deep_fork
    deep_fork = await fork1.afork(label="deep_fork")
    print(f"✅ Deep Fork -> {deep_fork.id}")

    # Step 6: Increment counter in deep_fork
    await deep_fork.aexec("redis-cli incr counter")
    print("🚀 Deep Fork: INCR counter")

    # ✅ Step 7: Verify each sandbox's counter
    print("\n🔍 Verifying counter values:")
    expected = {
        "Root": 1,
        "Fork 1": 2,
        "Deep Fork": 3,
    }

    for sandbox, label in [(root, "Root"), (fork1, "Fork 1"), (deep_fork, "Deep Fork")]:
        result = await sandbox.aexec("redis-cli get counter")
        value_str = result.get("stdout", "").strip()
        try:
            value = int(value_str)
        except ValueError:
            raise RuntimeError(f"❌ {label}: Unexpected Redis output: {value_str!r}")

        expected_val = expected[label]
        assert value == expected_val, f"❌ {label} counter expected {expected_val}, got {value}"
        print(f"✅ {label} counter == {value}")

    # 🚮 Cleanup
    print("\n🧹 Cleaning up...")
    for sbx in [deep_fork, fork1, root]:
        await sbx.aterminate()
        print(f"🛑 Terminated sandbox {sbx.id}")

asyncio.run(main())
