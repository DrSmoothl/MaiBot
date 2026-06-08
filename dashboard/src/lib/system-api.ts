import { fetchWithAuth, getAuthHeaders } from './fetch-with-auth'

/**
 * 系统控制 API
 */

/**
 * 重启麦麦主程序
 */
export async function restartMaiBot(): Promise<{ success: boolean; message: string }> {
  const response = await fetchWithAuth('/api/webui/system/restart', {
    method: 'POST',
    headers: getAuthHeaders(),
  })
  
  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '重启失败')
  }
  
  return await response.json()
}

/**
 * 检查麦麦运行状态
 */
export async function getMaiBotStatus(): Promise<{
  running: boolean
  uptime: number
  version: string
  start_time: string
}> {
  const response = await fetchWithAuth('/api/webui/system/status', {
    method: 'GET',
    headers: getAuthHeaders(),
  })
  
  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '获取状态失败')
  }
  
  return await response.json()
}

export interface CacheDirectoryStats {
  key: string
  label: string
  path: string
  exists: boolean
  file_count: number
  total_size: number
  db_records: number
}

export interface DatabaseFileStats {
  path: string
  exists: boolean
  size: number
}

export interface DatabaseTableStats {
  name: string
  rows: number
  size: number
  size_source: 'dbstat' | 'estimated'
  label: string
  category: string
  description: string
  cleanup_supported: boolean
  cleanup_date_column: string | null
}

export interface DatabaseStorageStats {
  files: DatabaseFileStats[]
  tables: DatabaseTableStats[]
  total_size: number
  page_size: number
  page_count: number
  freelist_count: number
  free_size: number
}

export interface LocalCacheStats {
  directories: CacheDirectoryStats[]
  database: DatabaseStorageStats
}

export type LocalCacheImageTarget = 'images' | 'emoji'

export interface LocalCacheImageItem {
  relative_path: string
  file_name: string
  full_path: string
  size: number
  modified_time: number
  format: string
  db_id: number | null
  image_hash: string | null
  description: string
  is_registered: boolean | null
  is_banned: boolean | null
  no_file_flag: boolean | null
}

export interface LocalCacheImageListResponse {
  success: boolean
  target: LocalCacheImageTarget
  total: number
  page: number
  page_size: number
  total_size: number
  data: LocalCacheImageItem[]
  date_groups: LocalCacheImageDateGroup[]
}

export interface LocalCacheImageDateGroup {
  date: string
  file_count: number
  total_size: number
}

export interface LocalCacheLogDirectoryItem {
  relative_path: string
  name: string
  full_path: string
  depth: number
  file_count: number
  total_size: number
  modified_time: number
  root_files_only: boolean
}

export interface LocalCacheLogDirectoryListResponse {
  success: boolean
  total: number
  data: LocalCacheLogDirectoryItem[]
}

export interface LocalCacheDataEntry {
  relative_path: string
  name: string
  full_path: string
  kind: 'file' | 'directory'
  file_count: number
  total_size: number
  modified_time: number
  protected: boolean
  protection_reason: string | null
}

export interface LocalCacheDataEntriesResponse {
  success: boolean
  root_path: string
  relative_path: string
  current_path: string
  parent_path: string | null
  file_count: number
  total_size: number
  total: number
  data: LocalCacheDataEntry[]
}

export interface LocalCacheCleanupResult {
  success: boolean
  message: string
  target: 'images' | 'emoji' | 'log_files' | 'database_logs' | 'data'
  removed_files: number
  removed_bytes: number
  removed_records: number
  vacuumed: boolean
  database_size_before: number | null
  database_size_after: number | null
  reclaimed_bytes: number
}

export type LocalCacheCleanupTarget = 'images' | 'emoji' | 'log_files' | 'database_logs'
export type DatabaseCleanupMode = 'all' | 'older_than_days'
export type DatabaseCleanupTable = string
export type LogCleanupTable = DatabaseCleanupTable

export interface LocalCacheDatabaseVacuumResult {
  success: boolean
  message: string
  database_size_before: number
  database_size_after: number
  reclaimed_bytes: number
  checkpoint_busy: number
  checkpoint_log: number
  checkpointed: number
}

export function getLocalCacheImagePreviewUrl(target: LocalCacheImageTarget, relativePath: string): string {
  const query = new URLSearchParams({
    target,
    relative_path: relativePath,
  })
  return `/api/webui/system/local-cache/images/preview?${query.toString()}`
}

export async function getLocalCacheStats(): Promise<LocalCacheStats> {
  const response = await fetchWithAuth('/api/webui/system/local-cache', {
    method: 'GET',
    headers: getAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '获取本地缓存统计失败')
  }

  return await response.json()
}

export async function getLocalCacheDatabaseStats(): Promise<DatabaseStorageStats> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/database', {
    method: 'GET',
    headers: getAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '获取数据库统计失败')
  }

  return await response.json()
}

export async function vacuumLocalCacheDatabase(): Promise<LocalCacheDatabaseVacuumResult> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/database/vacuum', {
    method: 'POST',
    headers: getAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '数据库 VACUUM 失败')
  }

  return await response.json()
}

export async function getLocalCacheDataEntries(relativePath = ''): Promise<LocalCacheDataEntriesResponse> {
  const query = new URLSearchParams()
  if (relativePath) {
    query.set('relative_path', relativePath)
  }

  const response = await fetchWithAuth(`/api/webui/system/local-cache/data-entries?${query.toString()}`, {
    method: 'GET',
    headers: getAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '获取 data 目录失败')
  }

  return await response.json()
}

export async function deleteLocalCacheDataEntry(relativePath: string): Promise<LocalCacheCleanupResult> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/data-entries', {
    method: 'DELETE',
    headers: {
      ...getAuthHeaders(),
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ relative_path: relativePath }),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '删除 data 条目失败')
  }

  return await response.json()
}

export async function cleanupLocalCache(
  target: LocalCacheCleanupTarget,
  tables: DatabaseCleanupTable[] = [],
  options: {
    database_mode?: DatabaseCleanupMode
    older_than_days?: number
    vacuum_after_cleanup?: boolean
  } = {}
): Promise<LocalCacheCleanupResult> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/cleanup', {
    method: 'POST',
    headers: {
      ...getAuthHeaders(),
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      target,
      tables,
      database_mode: options.database_mode ?? 'all',
      older_than_days: options.older_than_days ?? null,
      vacuum_after_cleanup: options.vacuum_after_cleanup ?? true,
    }),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '清理本地缓存失败')
  }

  return await response.json()
}

export async function getLocalCacheImages(params: {
  target: LocalCacheImageTarget
  page?: number
  page_size?: number
  start_date?: string
  end_date?: string
}): Promise<LocalCacheImageListResponse> {
  const query = new URLSearchParams({
    target: params.target,
    page: (params.page ?? 1).toString(),
    page_size: (params.page_size ?? 40).toString(),
  })
  if (params.start_date) {
    query.set('start_date', params.start_date)
  }
  if (params.end_date) {
    query.set('end_date', params.end_date)
  }

  const response = await fetchWithAuth(`/api/webui/system/local-cache/images?${query.toString()}`, {
    method: 'GET',
    headers: getAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '获取本地缓存图片列表失败')
  }

  return await response.json()
}

export async function deleteLocalCacheImage(
  target: LocalCacheImageTarget,
  relativePath: string
): Promise<LocalCacheCleanupResult> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/images', {
    method: 'DELETE',
    headers: {
      ...getAuthHeaders(),
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ target, relative_path: relativePath }),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '删除本地缓存图片失败')
  }

  return await response.json()
}

export async function deleteLocalCacheImagesByDateRange(
  target: LocalCacheImageTarget,
  startDate: string,
  endDate: string
): Promise<LocalCacheCleanupResult> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/images/bulk', {
    method: 'DELETE',
    headers: {
      ...getAuthHeaders(),
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      target,
      mode: 'date_range',
      start_date: startDate || null,
      end_date: endDate || null,
    }),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '按日期删除缓存失败')
  }

  return await response.json()
}

export async function deleteLocalCacheImagesOlderThanRecentDays(
  target: LocalCacheImageTarget,
  keepRecentDays: 1 | 7 | 30
): Promise<LocalCacheCleanupResult> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/images/bulk', {
    method: 'DELETE',
    headers: {
      ...getAuthHeaders(),
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      target,
      mode: 'older_than_recent_days',
      keep_recent_days: keepRecentDays,
    }),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '清理过期缓存失败')
  }

  return await response.json()
}

export async function getLocalCacheLogDirectories(): Promise<LocalCacheLogDirectoryListResponse> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/log-directories', {
    method: 'GET',
    headers: getAuthHeaders(),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '获取日志目录列表失败')
  }

  return await response.json()
}

export async function deleteLocalCacheLogDirectory(relativePath: string): Promise<LocalCacheCleanupResult> {
  const response = await fetchWithAuth('/api/webui/system/local-cache/log-directories', {
    method: 'DELETE',
    headers: {
      ...getAuthHeaders(),
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ relative_path: relativePath }),
  })

  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.detail || '清理日志目录失败')
  }

  return await response.json()
}
