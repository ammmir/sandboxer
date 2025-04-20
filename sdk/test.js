import { Sandboxer } from "./js/dist/index.js";

function getEnv(key) {
  // Deno
  if (typeof Deno !== "undefined" && typeof Deno.env !== "undefined") {
    const val = Deno.env.get(key);
    if (val) return val;
    throw new Error(`❌ Missing environment variable: ${key}`);
  }

  // Node.js / Bun
  if (typeof process !== "undefined" && process.env) {
    const val = process.env[key];
    if (val) return val;
    throw new Error(`❌ Missing environment variable: ${key}`);
  }

  throw new Error("❌ No environment support detected");
}

const BASE_URL = getEnv("SANDBOXER_BASE_URL");
const TOKEN = getEnv("SANDBOXER_TOKEN");

async function main() {
  const sandboxer = new Sandboxer(BASE_URL, TOKEN);

  // 🌱 Step 1: Create root sandbox
  const root = await sandboxer.create("redis", "ts-root", false);
  console.log(`✅ Started root sandbox ${root.id}`);

  // Step 2: Increment counter in root
  await root.exec("redis-cli incr counter");
  console.log("🚀 Root: INCR counter");

  // 🌿 Step 3: Fork → fork1
  const fork = await root.fork("fork");
  console.log(`✅ Forked → ${fork.id}`);

  // Step 4: Increment counter in fork
  await fork.exec("redis-cli incr counter");
  console.log("🚀 Fork: INCR counter");

  // 🍃 Step 5: Fork again → deep fork
  const deep = await fork.fork("deep");
  console.log(`✅ Deep Fork → ${deep.id}`);

  // Step 6: Increment counter in deep fork
  await deep.exec("redis-cli incr counter");
  console.log("🚀 Deep Fork: INCR counter");

  // 🔍 Step 7: Verify counter in each sandbox
  console.log("\n🔍 Verifying counter values:");
  const expected = {
    "Root": 1,
    "Fork": 2,
    "Deep": 3
  };

  const sandboxes = [
    [root, "Root"],
    [fork, "Fork"],
    [deep, "Deep"]
  ];

  for (const [sbx, label] of sandboxes) {
    const result = await sbx.exec("redis-cli get counter");
    const valueStr = result?.stdout?.trim() ?? "";
    const actual = parseInt(valueStr, 10);
    const expectedVal = expected[label];

    if (Number.isNaN(actual)) {
      console.error(`❌ ${label}: Invalid Redis output: ${valueStr}`);
    } else if (actual !== expectedVal) {
      console.error(`❌ ${label}: Expected ${expectedVal}, got ${actual}`);
    } else {
      console.log(`✅ ${label} counter == ${actual}`);
    }
  }

  // 🧹 Cleanup
  console.log("\n🧹 Cleaning up...");
  for (const sbx of [deep, fork, root]) {
    await sbx.terminate();
    console.log(`🛑 Terminated sandbox ${sbx.id}`);
  }
}

main().catch(err => {
  console.error("🔥 Error:", err);
  process.exit(1);
});
