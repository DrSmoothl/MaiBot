import type { ApiResponse } from '@/types/api'

import { fetchWithAuth, getAuthHeaders } from '@/lib/fetch-with-auth'
import { parseResponse } from '@/lib/api-helpers'

import type { InstalledPlugin, LegacyInstalledPlugin } from './types'

const INSTALLED_PLUGINS_CACHE_TTL = 1500

let installedPluginsCache: { timestamp: number; result: ApiResponse<InstalledPlugin[]> } | null = null
let installedPluginsRequest: Promise<ApiResponse<InstalledPlugin[]>> | null = null

/**
 * 获取已安装插件列表
 */
async function fetchInstalledPluginsUncached(): Promise<ApiResponse<InstalledPlugin[]>> {
  const response = await fetchWithAuth('/api/webui/plugins/installed', {
    headers: getAuthHeaders()
  })
  
  const apiResult = await parseResponse<{ success: boolean; plugins?: InstalledPlugin[]; message?: string }>(response)
  
  if (!apiResult.success) {
    return {
      success: true,
      data: []
    }
  }
  
  const result = apiResult.data
  if (!result.success) {
    return {
      success: true,
      data: []
    }
  }
  
  return {
    success: true,
    data: result.plugins || []
  }
}

export async function getInstalledPlugins(options: { forceRefresh?: boolean } = {}): Promise<ApiResponse<InstalledPlugin[]>> {
  if (
    !options.forceRefresh
    && installedPluginsCache
    && Date.now() - installedPluginsCache.timestamp < INSTALLED_PLUGINS_CACHE_TTL
  ) {
    return installedPluginsCache.result
  }

  if (!installedPluginsRequest || options.forceRefresh) {
    installedPluginsRequest = fetchInstalledPluginsUncached()
      .then((result) => {
        installedPluginsCache = { timestamp: Date.now(), result }
        return result
      })
      .finally(() => {
        installedPluginsRequest = null
      })
  }

  return installedPluginsRequest
}

/**
 * 检查插件是否已安装
 */
export function checkPluginInstalled(pluginId: string, installedPlugins: InstalledPlugin[]): boolean {
  return installedPlugins.some(p => p.id === pluginId)
}

/**
 * 获取已安装插件的版本
 */
export function getInstalledPluginVersion(pluginId: string, installedPlugins: (InstalledPlugin | LegacyInstalledPlugin)[]): string | undefined {
  const plugin = installedPlugins.find(p => p.id === pluginId)
  if (!plugin) return undefined
  
  // 兼容两种格式：新格式有 manifest，旧格式直接有 version
  if ('manifest' in plugin && plugin.manifest) {
    return plugin.manifest.version
  }
  // 旧版本格式
  if ('version' in plugin) {
    return plugin.version
  }
  return undefined
}
