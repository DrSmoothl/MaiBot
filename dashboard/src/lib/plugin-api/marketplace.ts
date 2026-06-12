import type { ApiResponse } from '@/types/api'
import type { PluginInfo, PluginType } from '@/types/plugin'

import { ApiError, backendApi, toApiResponse } from '@/lib/http'
import { pluginProgressClient } from '@/lib/plugin-progress-client'
import type { GitStatus, MaimaiVersion } from './types'

/**
 * 插件仓库配置
 */
const PLUGIN_REPO_OWNER = 'Mai-with-u'
const PLUGIN_REPO_NAME = 'plugin-repo'
const PLUGIN_REPO_BRANCH = 'main'
const PLUGIN_DETAILS_FILE = 'plugin_details.json'
const PLUGIN_LIST_CACHE_TTL = 5 * 60 * 1000
const PLUGIN_LIST_STORAGE_KEY = 'maibot-plugin-market-list-cache'
const PLUGIN_TYPES = new Set<PluginType>([
  'adapter',
  'chat',
  'creative',
  'provider',
  'management',
  'search',
  'knowledge',
  'media',
  'game',
  'security',
  'automation',
  'extension',
  'other',
])

let pluginListCache: { timestamp: number; result: ApiResponse<PluginInfo[]> } | null = null
let pluginListRequest: Promise<ApiResponse<PluginInfo[]>> | null = null

interface PluginListStorageCache {
  timestamp: number
  data: PluginInfo[]
}

/**
 * 插件列表 API 响应类型（只包含我们需要的字段）
 */
interface PluginApiResponse {
  id?: string
  assets?: PluginInfo['assets']
  manifest: {
    manifest_version: number
    id?: string
    name: string
    version: string
    description: string
    author: {
      name: string
      url?: string
    }
    license: string
    host_application: {
      min_version: string
      max_version?: string
    }
    homepage_url?: string
    repository_url?: string
    urls?: {
      repository?: string
      homepage?: string
      documentation?: string
      issues?: string
    }
    keywords: string[]
    plugin_type?: string
    display?: PluginInfo['manifest']['display']
    default_locale: string
    locales_path?: string
  }
  // 可能还有其他字段,但我们不关心
  [key: string]: unknown
}

function uniqueNonEmptyValues(values: Array<string | undefined>): string[] {
  return Array.from(new Set(values.map(value => value?.trim()).filter((value): value is string => Boolean(value))))
}

function normalizePluginType(value: unknown): PluginType {
  if (typeof value !== 'string' || !value.trim()) {
    return 'extension'
  }

  const normalizedValue = value.trim()
  if (PLUGIN_TYPES.has(normalizedValue as PluginType)) {
    return normalizedValue as PluginType
  }

  return 'other'
}

function normalizePluginManifest(manifest: PluginApiResponse['manifest']): PluginInfo['manifest'] {
  const repositoryUrl = manifest.repository_url || manifest.urls?.repository
  const homepageUrl = manifest.homepage_url || manifest.urls?.homepage

  return {
    manifest_version: manifest.manifest_version || 1,
    id: manifest.id,
    name: manifest.name,
    version: manifest.version,
    description: manifest.description || '',
    author: manifest.author || { name: 'Unknown' },
    license: manifest.license || 'Unknown',
    host_application: manifest.host_application || { min_version: '0.0.0' },
    homepage_url: homepageUrl,
    repository_url: repositoryUrl,
    urls: manifest.urls,
    keywords: manifest.keywords || [],
    plugin_type: normalizePluginType(manifest.plugin_type),
    display: manifest.display,
    default_locale: manifest.default_locale || 'zh-CN',
    locales_path: manifest.locales_path,
  }
}

function normalizePluginAssetUrl(value: unknown): string | undefined {
  if (typeof value !== 'string' || !value.trim()) {
    return undefined
  }

  const normalizedValue = value.trim()
  if (/^https?:\/\//.test(normalizedValue)) {
    return normalizedValue
  }

  const normalizedPath = normalizedValue.replace(/^\/+/, '')
  if (!normalizedPath || normalizedPath.includes('..')) {
    return undefined
  }

  return `https://raw.githubusercontent.com/${PLUGIN_REPO_OWNER}/${PLUGIN_REPO_NAME}/${PLUGIN_REPO_BRANCH}/${normalizedPath}`
}

function normalizePluginAssets(assets: PluginApiResponse['assets']): PluginInfo['assets'] | undefined {
  const icon64 = normalizePluginAssetUrl(assets?.icon_64)
  if (!icon64) {
    return undefined
  }

  return {
    icon_64: icon64,
  }
}

function normalizeDateString(value: unknown): string {
  if (typeof value === 'string') {
    return value
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return new Date(value).toISOString()
  }

  return ''
}

function readPluginListStorageCache(): PluginListStorageCache | null {
  if (typeof localStorage === 'undefined') {
    return null
  }

  try {
    const rawCache = localStorage.getItem(PLUGIN_LIST_STORAGE_KEY)
    if (!rawCache) {
      return null
    }

    const cache = JSON.parse(rawCache) as Partial<PluginListStorageCache>
    if (!cache.timestamp || !Array.isArray(cache.data)) {
      return null
    }

    return {
      timestamp: Number(cache.timestamp),
      data: cache.data,
    }
  } catch (error) {
    console.warn('读取插件市场缓存失败:', error)
    return null
  }
}

function writePluginListStorageCache(data: PluginInfo[]): void {
  if (typeof localStorage === 'undefined') {
    return
  }

  try {
    localStorage.setItem(
      PLUGIN_LIST_STORAGE_KEY,
      JSON.stringify({
        timestamp: Date.now(),
        data,
      })
    )
  } catch (error) {
    console.warn('写入插件市场缓存失败:', error)
  }
}

export function getCachedPluginList(): PluginInfo[] | null {
  if (pluginListCache?.result.success) {
    return pluginListCache.result.data
  }

  const storedCache = readPluginListStorageCache()
  if (!storedCache) {
    return null
  }

  const result: ApiResponse<PluginInfo[]> = { success: true, data: storedCache.data }
  pluginListCache = { timestamp: storedCache.timestamp, result }
  return storedCache.data
}

/**
 * 从远程获取插件列表(通过后端代理避免 CORS)
 */
async function fetchPluginListUncached(): Promise<ApiResponse<PluginInfo[]>> {
  return toApiResponse(async () => {
    const result = await backendApi.post<{ success: boolean; data: string; error?: string }>(
      '/api/webui/plugins/fetch-raw',
      {
        body: {
          owner: PLUGIN_REPO_OWNER,
          repo: PLUGIN_REPO_NAME,
          branch: PLUGIN_REPO_BRANCH,
          file_path: PLUGIN_DETAILS_FILE
        },
        errorMessage: '获取插件列表失败',
      }
    )

    // 业务级失败：该 endpoint 的错误字段是 error 而非 message，不走 requireSuccess
    if (!result.success || !result.data) {
      throw new ApiError(result.error || '获取插件列表失败', { detail: result })
    }

    const data: PluginApiResponse[] = JSON.parse(result.data)

    const pluginList = data
      .filter(item => {
        if (!item?.manifest) {
          console.warn('跳过无效插件数据:', item)
          return false
        }
        const pluginId = item.manifest.id || item.id
        if (!pluginId) {
          console.warn('跳过缺少 ID 的插件:', item)
          return false
        }
        if (!item.manifest.name || !item.manifest.version) {
          console.warn('跳过缺少必需字段的插件:', item.id)
          return false
        }
        return true
      })
      .map((item, index) => {
        const manifestId = item.manifest.id?.trim()
        const marketplaceId = item.id?.trim()
        const pluginId = manifestId || marketplaceId!

        return {
          id: pluginId,
          marketplace_id: marketplaceId,
          marketplace_order: index,
          stats_ids: uniqueNonEmptyValues([manifestId]),
          manifest: normalizePluginManifest({ ...item.manifest, id: pluginId }),
          assets: normalizePluginAssets(item.assets),
          downloads: 0,
          rating: 0,
          review_count: 0,
          installed: false,
          source: 'market' as const,
          published_at: normalizeDateString(item.published_at ?? item.created_at ?? item.added_at),
          updated_at: normalizeDateString(item.updated_at ?? item.modified_at),
        }
      })

    return pluginList
  })
}

export async function fetchPluginList(options: { forceRefresh?: boolean } = {}): Promise<ApiResponse<PluginInfo[]>> {
  if (
    !options.forceRefresh
    && pluginListCache
    && Date.now() - pluginListCache.timestamp < PLUGIN_LIST_CACHE_TTL
  ) {
    return pluginListCache.result
  }

  if (!options.forceRefresh && !pluginListCache) {
    const storedCache = readPluginListStorageCache()
    if (storedCache && Date.now() - storedCache.timestamp < PLUGIN_LIST_CACHE_TTL) {
      const result: ApiResponse<PluginInfo[]> = { success: true, data: storedCache.data }
      pluginListCache = { timestamp: storedCache.timestamp, result }
      return result
    }
  }

  if (!pluginListRequest || options.forceRefresh) {
    pluginListRequest = fetchPluginListUncached()
      .then((result) => {
        if (result.success) {
          pluginListCache = { timestamp: Date.now(), result }
          writePluginListStorageCache(result.data)
        }
        return result
      })
      .finally(() => {
        pluginListRequest = null
      })
  }

  return pluginListRequest
}

/**
 * 检查本机 Git 安装状态
 */
export async function checkGitStatus(): Promise<ApiResponse<GitStatus>> {
  try {
    const data = await backendApi.get<GitStatus>('/api/webui/plugins/git-status', {
      errorMessage: '无法检测 Git 安装状态',
    })
    return { success: true, data }
  } catch (error) {
    // 保持原有行为：HTTP 错误 / 响应解析失败时按“无法检测”处理；网络层失败与认证失效（401）仍向上抛出
    if (error instanceof ApiError && error.status !== undefined && error.status !== 401) {
      return {
        success: true,
        data: {
          installed: false,
          error: '无法检测 Git 安装状态'
        }
      }
    }
    throw error
  }
}

/**
 * 获取麦麦版本信息
 */
export async function getMaimaiVersion(): Promise<ApiResponse<MaimaiVersion>> {
  try {
    const data = await backendApi.get<MaimaiVersion>('/api/webui/plugins/version', {
      errorMessage: '获取麦麦版本信息失败',
    })
    return { success: true, data }
  } catch (error) {
    // 保持原有行为：HTTP 错误 / 响应解析失败时回退为 0.0.0；网络层失败与认证失效（401）仍向上抛出
    if (error instanceof ApiError && error.status !== undefined && error.status !== 401) {
      return {
        success: true,
        data: {
          version: '0.0.0',
          version_major: 0,
          version_minor: 0,
          version_patch: 0
        }
      }
    }
    throw error
  }
}

/**
 * 比较版本号
 * 
 * @param pluginMinVersion 插件要求的最小版本
 * @param pluginMaxVersion 插件要求的最大版本(可选)
 * @param maimaiVersion 麦麦当前版本
 * @returns true 表示兼容,false 表示不兼容
 */
export function isPluginCompatible(
  pluginMinVersion: string,
  pluginMaxVersion: string | undefined,
  maimaiVersion: MaimaiVersion
): boolean {
  // 解析插件最小版本
  const minParts = pluginMinVersion.split('.').map(p => parseInt(p) || 0)
  const minMajor = minParts[0] || 0
  const minMinor = minParts[1] || 0
  const minPatch = minParts[2] || 0
  
  // 检查最小版本
  if (maimaiVersion.version_major < minMajor) return false
  if (maimaiVersion.version_major === minMajor && maimaiVersion.version_minor < minMinor) return false
  if (maimaiVersion.version_major === minMajor && 
      maimaiVersion.version_minor === minMinor && 
      maimaiVersion.version_patch < minPatch) return false
  
  // 检查最大版本(如果有)
  if (pluginMaxVersion) {
    const maxParts = pluginMaxVersion.split('.').map(p => parseInt(p) || 0)
    const maxMajor = maxParts[0] || 0
    const maxMinor = maxParts[1] || 0
    const maxPatch = maxParts[2] || 0
    
    if (maimaiVersion.version_major > maxMajor) return false
    if (maimaiVersion.version_major === maxMajor && maimaiVersion.version_minor > maxMinor) return false
    if (maimaiVersion.version_major === maxMajor && 
        maimaiVersion.version_minor === maxMinor && 
        maimaiVersion.version_patch > maxPatch) return false
  }
  
  return true
}

/**
 * 连接插件加载进度 WebSocket
 * 
 * 使用临时 token 进行认证,异步获取 token 后连接
 */
export async function connectPluginProgressWebSocket(
  onProgress: (progress: import('./types').PluginLoadProgress) => void,
  onError?: (error: Error) => void
): Promise<() => Promise<void>> {
  try {
    return await pluginProgressClient.subscribe(onProgress)
  } catch (error) {
    const normalizedError = error instanceof Error ? error : new Error('插件进度订阅失败')
    onError?.(normalizedError)
    return async () => {}
  }
}
