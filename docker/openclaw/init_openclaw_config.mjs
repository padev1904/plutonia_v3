import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

function truthy(value, defaultValue = false) {
  if (value == null) {
    return defaultValue;
  }
  return ["1", "true", "yes", "on"].includes(String(value).trim().toLowerCase());
}

function csvList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function uniqueList(items) {
  return Array.from(new Set((items || []).map((item) => String(item).trim()).filter(Boolean)));
}

const configDir = process.env.OPENCLAW_CONFIG_DIR || "/root/.openclaw";
const workspaceDir = process.env.OPENCLAW_AGENT_WORKSPACE || process.env.OPENCLAW_WORKSPACE_DIR || path.join(configDir, "workspace");
const configPath = path.join(configDir, "openclaw.json");

fs.mkdirSync(configDir, { recursive: true });
fs.mkdirSync(workspaceDir, { recursive: true });
fs.mkdirSync(path.join(configDir, "logs"), { recursive: true });
fs.mkdirSync(path.join(configDir, "telegram"), { recursive: true });

const nowIso = new Date().toISOString();
const providerName = process.env.OPENCLAW_PROVIDER_NAME || "ollama-2";
const ollamaBaseUrl = process.env.OLLAMA_BASE_URL || process.env.OLLAMA_HOST || "http://ollama:11434";
const ollamaApiKey = process.env.OLLAMA_API_KEY || "ollama-local";
const modelId = process.env.OPENCLAW_MODEL_ID || "qwen3:14b-16k";
const modelName = process.env.OPENCLAW_MODEL_NAME || modelId;
const modelContextWindow = Number.parseInt(process.env.OPENCLAW_MODEL_CONTEXT_WINDOW || "16384", 10) || 16384;
const modelMaxTokens = Number.parseInt(process.env.OPENCLAW_MODEL_MAX_TOKENS || String(modelContextWindow), 10) || modelContextWindow;
const agentTimeoutSeconds = Number.parseInt(process.env.OPENCLAW_AGENT_TIMEOUT_SECONDS || "0", 10) || 0;
const gatewayToken = (process.env.OPENCLAW_GATEWAY_TOKEN || "").trim() || crypto.randomBytes(24).toString("hex");
const gatewayRemoteUrl = (process.env.OPENCLAW_GATEWAY_REMOTE_URL || "ws://openclaw:18789").trim();
const gatewayBind = (process.env.OPENCLAW_GATEWAY_BIND || "").trim();
const gatewayControlUiAllowedOrigins = uniqueList(
  csvList(process.env.OPENCLAW_GATEWAY_CONTROL_UI_ALLOWED_ORIGINS || ""),
);
const gatewayControlUiAllowHostHeaderFallback = truthy(
  process.env.OPENCLAW_GATEWAY_CONTROL_UI_ALLOW_HOST_HEADER_FALLBACK,
  false,
);
const telegramEnabled = truthy(process.env.OPENCLAW_TELEGRAM_ENABLED, false);
const telegramBotToken = (process.env.OPENCLAW_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN || "").trim();
const telegramAllowFrom = uniqueList(
  csvList(process.env.OPENCLAW_TELEGRAM_ALLOW_FROM || process.env.TELEGRAM_CHAT_ID || ""),
);
const telegramDmPolicy = (process.env.OPENCLAW_TELEGRAM_DM_POLICY || (telegramAllowFrom.length ? "allowlist" : "pairing")).trim();
const telegramGroupPolicy = (process.env.OPENCLAW_TELEGRAM_GROUP_POLICY || "allowlist").trim();
const toolsProfile = (process.env.OPENCLAW_TOOLS_PROFILE || "").trim();
const toolsAllow = uniqueList(csvList(process.env.OPENCLAW_TOOLS_ALLOW || ""));
const toolsDeny = uniqueList(csvList(process.env.OPENCLAW_TOOLS_DENY || ""));
const webFetchEnabled = truthy(process.env.OPENCLAW_WEB_FETCH_ENABLED, true);
const webSearchEnabled = truthy(process.env.OPENCLAW_WEB_SEARCH_ENABLED, false);
const searchProvider = (process.env.OPENCLAW_WEB_SEARCH_PROVIDER || "").trim();
const searchApiKey = (process.env.OPENCLAW_WEB_SEARCH_API_KEY || process.env.BRAVE_API_KEY || "").trim();

const config = fs.existsSync(configPath)
  ? JSON.parse(fs.readFileSync(configPath, "utf8"))
  : {};
const persistedGatewayToken = String(config.gateway?.auth?.token || config.gateway?.remote?.token || "").trim();
const effectiveGatewayToken = gatewayToken || persistedGatewayToken || crypto.randomBytes(24).toString("hex");

config.meta = {
  ...(config.meta || {}),
  lastTouchedVersion: "docker-bootstrap",
  lastTouchedAt: nowIso,
};

config.wizard = {
  ...(config.wizard || {}),
  lastRunAt: nowIso,
  lastRunVersion: "docker-bootstrap",
  lastRunCommand: "bootstrap",
  lastRunMode: "local",
};

config.models = {
  ...(config.models || {}),
  mode: "merge",
  providers: {
    ...((config.models && config.models.providers) || {}),
    [providerName]: {
      ...(((config.models || {}).providers || {})[providerName] || {}),
      baseUrl: ollamaBaseUrl,
      apiKey: ollamaApiKey,
      api: "ollama",
      models: [
        {
          id: modelId,
          name: modelName,
          contextWindow: modelContextWindow,
          maxTokens: modelMaxTokens,
        },
      ],
    },
  },
};

config.agents = config.agents || {};
config.agents.defaults = {
  ...(config.agents.defaults || {}),
  model: `${providerName}/${modelId}`,
  workspace: workspaceDir,
  skipBootstrap: true,
  ...(agentTimeoutSeconds > 0 ? { timeoutSeconds: agentTimeoutSeconds } : {}),
  models: {
    ...((config.agents.defaults || {}).models || {}),
    [`${providerName}/${modelId}`]: {
      alias: "openclaw_default",
    },
  },
  compaction: {
    ...((config.agents.defaults || {}).compaction || {}),
    mode: "safeguard",
  },
};

config.tools = config.tools || {};
if (toolsProfile) {
  config.tools.profile = toolsProfile;
}
if (toolsAllow.length) {
  config.tools.allow = uniqueList([...(config.tools.allow || []), ...toolsAllow]);
}
if (toolsDeny.length) {
  config.tools.deny = uniqueList([...(config.tools.deny || []), ...toolsDeny]);
}
config.tools.web = {
  ...(config.tools.web || {}),
  fetch: {
    ...((config.tools.web || {}).fetch || {}),
    enabled: webFetchEnabled,
  },
  search: {
    ...((config.tools.web || {}).search || {}),
    enabled: webSearchEnabled,
  },
};
if (searchProvider) {
  config.tools.web.search.provider = searchProvider;
}
if (searchApiKey) {
  config.tools.web.search.apiKey = searchApiKey;
}
config.tools.loopDetection = {
  ...((config.tools.loopDetection || {})),
  enabled: true,
};

config.commands = {
  ...(config.commands || {}),
  native: "auto",
  nativeSkills: "auto",
  restart: true,
  ownerDisplay: "raw",
};

config.channels = config.channels || {};
config.channels.telegram = {
  ...(config.channels.telegram || {}),
  enabled: Boolean(telegramEnabled && telegramBotToken),
  dmPolicy: telegramDmPolicy,
  botToken: telegramBotToken,
  allowFrom: telegramAllowFrom,
  groupPolicy: telegramGroupPolicy,
  streaming: "off",
};

config.gateway = config.gateway || {};
config.gateway.mode = "local";
if (gatewayBind) {
  config.gateway.bind = gatewayBind;
}
config.gateway.auth = {
  ...(config.gateway.auth || {}),
  mode: "token",
  token: effectiveGatewayToken,
};
config.gateway.remote = {
  ...(config.gateway.remote || {}),
  url: gatewayRemoteUrl,
  token: effectiveGatewayToken,
};
config.gateway.controlUi = {
  ...(config.gateway.controlUi || {}),
};
if (gatewayControlUiAllowedOrigins.length) {
  config.gateway.controlUi.allowedOrigins = gatewayControlUiAllowedOrigins;
}
if (gatewayControlUiAllowHostHeaderFallback) {
  config.gateway.controlUi.dangerouslyAllowHostHeaderOriginFallback = true;
}

fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
