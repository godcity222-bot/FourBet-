const forbiddenPatchEnvVars = [
  "BRIDGE_DEPLOY_LABEL",
  "BRIDGE_INDEX_B64",
  "BRIDGE_NORMALIZE_B64",
  "BRIDGE_PACKAGE_B64",
  "BOOT_PATCH",
  "BRIDGE_PATCH",
  "ODDS_API_BRIDGE",
  "ODDS_API_BRIDGE_CODE",
  "INDEX_JS",
  "PATCH",
  "BRIDGE_VERSION",
];

const active = forbiddenPatchEnvVars.filter((name) => Boolean(process.env[name]));
if (active.length) {
  console.error(`[preflight] refused to start: remove legacy Railway variable(s): ${active.join(", ")}`);
  console.error("[preflight] those variables can inject the old odds-api-bridge v2.0.24 with markets=20/ws_chunks=1");
  process.exit(1);
}

console.log("[preflight] OK: no legacy bridge patch variables detected");