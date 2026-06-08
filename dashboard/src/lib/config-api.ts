/**
 * 配置API客户端
 */

import { parseResponse } from '@/lib/api-helpers'
import { fetchWithAuth } from '@/lib/fetch-with-auth'
import type { ApiResponse } from '@/types/api'
import type { ConfigSchema } from '@/types/config-schema'

const API_BASE = '/api/webui/config'
export const BOT_CONFIG_UPDATED_EVENT = 'maibot:bot-config-updated'
const schemaRequestCache = new Map<string, Promise<ApiResponse<ConfigSchema>>>()
const configDataCache = new Map<string, { timestamp: number; request: Promise<ApiResponse<Record<string, unknown>>> }>()
const CONFIG_DATA_CACHE_TTL = 30_000

function unwrapConfigResponse(data: unknown): Record<string, unknown> {
  if (
    data &&
    typeof data === 'object' &&
    'config' in data &&
    (data as Record<string, unknown>).config &&
    typeof (data as Record<string, unknown>).config === 'object'
  ) {
    return (data as { config: Record<string, unknown> }).config
  }

  if (data && typeof data === 'object') {
    return data as Record<string, unknown>
  }

  return {}
}

function getCachedSchema(key: string, url: string): Promise<ApiResponse<ConfigSchema>> {
  const cachedRequest = schemaRequestCache.get(key)
  if (cachedRequest) {
    return cachedRequest
  }

  const request = fetchWithAuth(url, { cache: 'no-store' })
    .then((response) => parseResponse<ConfigSchema>(response))
    .catch((error) => {
      schemaRequestCache.delete(key)
      throw error
    })

  schemaRequestCache.set(key, request)
  return request
}

function getCachedConfigData(key: string, url: string): Promise<ApiResponse<Record<string, unknown>>> {
  const cachedRequest = configDataCache.get(key)
  if (cachedRequest && Date.now() - cachedRequest.timestamp < CONFIG_DATA_CACHE_TTL) {
    return cachedRequest.request
  }

  const request = fetchWithAuth(url, { cache: 'no-store' })
    .then((response) => parseResponse<unknown>(response))
    .then((result): ApiResponse<Record<string, unknown>> => (
      result.success ? { success: true, data: unwrapConfigResponse(result.data) } : result
    ))
    .catch((error) => {
      configDataCache.delete(key)
      throw error
    })

  configDataCache.set(key, { timestamp: Date.now(), request })
  return request
}

function invalidateConfigDataCache(key?: string): void {
  if (key) {
    configDataCache.delete(key)
    return
  }
  configDataCache.clear()
}

function notifyBotConfigUpdated(): void {
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new Event(BOT_CONFIG_UPDATED_EVENT))
  }
}

/**
 * 获取麦麦主程序配置架构
 */
export async function getBotConfigSchema(): Promise<ApiResponse<ConfigSchema>> {
  return getCachedSchema('bot', `${API_BASE}/schema/bot`)
}

/**
 * 获取模型配置架构
 */
export async function getModelConfigSchema(): Promise<ApiResponse<ConfigSchema>> {
  return getCachedSchema('model', `${API_BASE}/schema/model`)
}

/**
 * 获取指定配置节的架构
 */
export async function getConfigSectionSchema(sectionName: string): Promise<ApiResponse<ConfigSchema>> {
  return getCachedSchema(`section:${sectionName}`, `${API_BASE}/schema/section/${sectionName}`)
}

/**
 * 获取麦麦主程序配置数据
 */
export async function getBotConfig(): Promise<ApiResponse<Record<string, unknown>>> {
  const response = await fetchWithAuth(`${API_BASE}/bot`, { cache: 'no-store' })
  const result = await parseResponse<unknown>(response)
  return result.success ? { success: true, data: unwrapConfigResponse(result.data) } : result
}

/** Cached config data for lightweight status summaries. */
export async function getBotConfigCached(): Promise<ApiResponse<Record<string, unknown>>> {
  return getCachedConfigData('bot', `${API_BASE}/bot`)
}

/**
 * 获取模型配置数据
 */
export async function getModelConfig(): Promise<ApiResponse<Record<string, unknown>>> {
  const response = await fetchWithAuth(`${API_BASE}/model`, { cache: 'no-store' })
  const result = await parseResponse<unknown>(response)
  return result.success ? { success: true, data: unwrapConfigResponse(result.data) } : result
}

/** Cached model config data for lightweight status summaries. */
export async function getModelConfigCached(): Promise<ApiResponse<Record<string, unknown>>> {
  return getCachedConfigData('model', `${API_BASE}/model`)
}

/**
 * 更新麦麦主程序配置
 */
export async function updateBotConfig(
  config: Record<string, unknown>
): Promise<ApiResponse<Record<string, unknown>>> {
  const response = await fetchWithAuth(`${API_BASE}/bot`, {
    method: 'POST',
    body: JSON.stringify(config),
  })
  const result = await parseResponse<Record<string, unknown>>(response)
  if (result.success) {
    invalidateConfigDataCache('bot')
    notifyBotConfigUpdated()
  }
  return result
}

/**
 * 获取麦麦主程序配置的原始 TOML 内容
 */
export async function getBotConfigRaw(): Promise<ApiResponse<string>> {
  const response = await fetchWithAuth(`${API_BASE}/bot/raw`, { cache: 'no-store' })
  return parseResponse<string>(response)
}

/**
 * 更新麦麦主程序配置（原始 TOML 内容）
 */
export async function updateBotConfigRaw(rawContent: string): Promise<ApiResponse<Record<string, unknown>>> {
  const response = await fetchWithAuth(`${API_BASE}/bot/raw`, {
    method: 'POST',
    body: JSON.stringify({ raw_content: rawContent }),
  })
  const result = await parseResponse<Record<string, unknown>>(response)
  if (result.success) {
    invalidateConfigDataCache('bot')
    notifyBotConfigUpdated()
  }
  return result
}

/**
 * 更新模型配置
 */
export async function updateModelConfig(
  config: Record<string, unknown>
): Promise<ApiResponse<Record<string, unknown>>> {
  const response = await fetchWithAuth(`${API_BASE}/model`, {
    method: 'POST',
    body: JSON.stringify(config),
  })
  const result = await parseResponse<Record<string, unknown>>(response)
  if (result.success) invalidateConfigDataCache('model')
  return result
}

/**
 * 更新麦麦主程序配置的指定节
 */
export async function updateBotConfigSection(
  sectionName: string,
  sectionData: unknown
): Promise<ApiResponse<Record<string, unknown>>> {
  const response = await fetchWithAuth(`${API_BASE}/bot/section/${sectionName}`, {
    method: 'POST',
    body: JSON.stringify(sectionData),
  })
  const result = await parseResponse<Record<string, unknown>>(response)
  if (result.success) {
    invalidateConfigDataCache('bot')
    notifyBotConfigUpdated()
  }
  return result
}

/**
 * 更新模型配置的指定节
 */
export async function updateModelConfigSection(
  sectionName: string,
  sectionData: unknown
): Promise<ApiResponse<Record<string, unknown>>> {
  const response = await fetchWithAuth(`${API_BASE}/model/section/${sectionName}`, {
    method: 'POST',
    body: JSON.stringify(sectionData),
  })
  const result = await parseResponse<Record<string, unknown>>(response)
  if (result.success) invalidateConfigDataCache('model')
  return result
}

/**
 * 模型信息
 */
export interface ModelListItem {
  id: string
  name: string
  owned_by?: string
}

/**
 * 获取模型列表响应
 */
export interface FetchModelsResponse {
  success: boolean
  models: ModelListItem[]
  provider?: string
  count: number
}

/**
 * 已注册的模型客户端类型
 */
export interface ModelClientType {
  client_type: string
  owner_plugin_id: string | null
  version: string
  description: string
  builtin: boolean
}

/**
 * 获取当前主程序与插件已注册的模型客户端类型
 */
export async function fetchModelClientTypes(): Promise<ApiResponse<ModelClientType[]>> {
  const response = await fetchWithAuth('/api/webui/models/client-types')
  const parsed = await parseResponse<{ client_types?: ModelClientType[] } | ModelClientType[]>(response)
  if (!parsed.success) {
    return parsed
  }
  const body = parsed.data
  const clientTypes = Array.isArray(body) ? body : Array.isArray(body?.client_types) ? body.client_types : []
  return { success: true, data: clientTypes }
}

/**
 * 获取指定提供商的可用模型列表
 * @param providerName 提供商名称（在 model_config.toml 中配置的名称）
 * @param parser 响应解析器类型 ('openai' | 'gemini')
 * @param endpoint 获取模型列表的端点（默认 '/models'）
 */
export async function fetchProviderModels(
  providerName: string,
  parser: 'openai' | 'gemini' = 'openai',
  endpoint: string = '/models'
): Promise<ApiResponse<ModelListItem[]>> {
  const params = new URLSearchParams({
    provider_name: providerName,
    parser,
    endpoint,
  })
  const response = await fetchWithAuth(`/api/webui/models/list?${params}`)
  // 后端返回 { success, models, provider, count }，需要展开取出 models 数组
  const parsed = await parseResponse<{ models?: ModelListItem[] } | ModelListItem[]>(response)
  if (!parsed.success) {
    return parsed
  }
  const body = parsed.data
  const models = Array.isArray(body) ? body : Array.isArray(body?.models) ? body.models : []
  return { success: true, data: models }
}

/**
 * 测试提供商连接结果
 */
export interface TestConnectionResult {
  network_ok: boolean
  api_key_valid: boolean | null
  latency_ms: number | null
  error: string | null
  http_status: number | null
}

/**
 * 测试提供商连接状态（通过提供商名称）
 * @param providerName 提供商名称
 */
export async function testProviderConnection(
  providerName: string
): Promise<ApiResponse<TestConnectionResult>> {
  const params = new URLSearchParams({
    provider_name: providerName,
  })
  const response = await fetchWithAuth(`/api/webui/models/test-connection-by-name?${params}`, {
    method: 'POST',
  })
  return parseResponse<TestConnectionResult>(response)
}
