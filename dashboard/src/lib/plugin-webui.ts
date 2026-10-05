import { Activity, Database, List, Puzzle, Settings } from 'lucide-react'
import { useEffect, useSyncExternalStore } from 'react'

import { backendApi } from '@/lib/http'
import { onBackendUrlChanged } from '@/lib/api-base'

export type Scalar = string | number | boolean | null
export interface DataReference {
  source: string
  field: string
}
export interface Parameter {
  type: 'string' | 'integer' | 'number' | 'boolean'
  required: boolean
  max_length: number
  minimum: number | null
  maximum: number | null
  choices: Scalar[]
}
export interface APIBinding {
  api: string
  version: string
  parameters: Record<string, Parameter>
  confirmation: string | null
}
export interface WebUINode {
  type:
    | 'stack'
    | 'grid'
    | 'card'
    | 'tabs'
    | 'text'
    | 'stat'
    | 'table'
    | 'chart'
    | 'input'
    | 'select'
    | 'switch'
    | 'date'
    | 'button'
  label: string | null
  value: Scalar | DataReference
  children: WebUINode[]
  columns: number | Array<{ field: string; label: string }> | null
  name: string | null
  options: Array<{ label: string; value: string }>
  action: string | null
  variant: 'primary' | 'danger' | 'muted'
  chart_type: 'line' | 'bar'
  x: string | null
  y: string | null
}
export interface WebUIPage {
  id: string
  title: string
  description: string
  placement: 'sidebar' | 'workspace'
  icon: keyof typeof extensionIcons
  queries: Record<string, APIBinding>
  actions: Record<string, APIBinding>
  content: WebUINode[]
}
export interface WebUIExtension {
  plugin_id: string
  workspace_title: string | null
  pages: WebUIPage[]
}
export const extensionIcons = {
  puzzle: Puzzle,
  chart: Activity,
  settings: Settings,
  database: Database,
  list: List,
}
export const extensionPath = (pluginId: string, pageId: string) =>
  `/extensions/${encodeURIComponent(pluginId)}/${encodeURIComponent(pageId)}`
export const extensionWorkspace = (pluginId: string) => `plugin:${pluginId}` as const

interface Preferences {
  hidden: string[]
  order: string[]
}
interface RegistryState {
  extensions: WebUIExtension[]
  loading: boolean
  error: string | null
  preferences: Preferences
}
const STORAGE_KEY = 'maibot-plugin-webui-preferences'
const listeners = new Set<() => void>()
let state: RegistryState = {
  extensions: [],
  loading: true,
  error: null,
  preferences: { hidden: [], order: [] },
}
let generation = 0
let pending: Promise<void> | null = null
let activeUsers = 0
let registryTimer: number | null = null
let unsubscribeBackend: (() => void) | null = null

function publish(next: RegistryState) {
  state = next
  listeners.forEach((listener) => listener())
}
function readPreferences(): Preferences {
  const raw = localStorage.getItem(STORAGE_KEY)
  if (!raw) return { hidden: [], order: [] }
  try {
    const parsed = JSON.parse(raw)
    if (
      Array.isArray(parsed.hidden) &&
      parsed.hidden.every((item: unknown) => typeof item === 'string') &&
      Array.isArray(parsed.order) &&
      parsed.order.every((item: unknown) => typeof item === 'string')
    )
      return parsed
  } catch {
    /* 损坏的本地显示偏好不影响插件注册和后端权限。 */
  }
  return { hidden: [], order: [] }
}

export function refreshPluginWebUI(): Promise<void> {
  if (pending) return pending
  const current = generation
  pending = backendApi
    .get<{ extensions: WebUIExtension[] }>('/api/webui/plugins/runtime/webui')
    .then((response) => {
      if (current === generation) {
        // 未变更的声明保留引用，注册表轮询不会中断页面中进行的操作或重置表单。
        const extensions = response.extensions.map((extension) => {
          const previous = state.extensions.find((item) => item.plugin_id === extension.plugin_id)
          return previous && JSON.stringify(previous) === JSON.stringify(extension)
            ? previous
            : extension
        })
        publish({ ...state, extensions, loading: false, error: null })
      }
    })
    .catch((error: unknown) => {
      if (current === generation)
        publish({ ...state, extensions: [], loading: false, error: String(error) })
    })
    .finally(() => {
      if (current === generation) pending = null
    })
  return pending
}

export function setPluginWebUIPreferences(preferences: Preferences) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(preferences))
  publish({ ...state, preferences })
}

/** 所有导航共享一次注册表加载；定期刷新以反映插件卸载和重载。 */
export function usePluginWebUI(enabled = false): RegistryState {
  const snapshot = useSyncExternalStore(
    (listener) => {
      listeners.add(listener)
      return () => {
        listeners.delete(listener)
      }
    },
    () => state
  )
  useEffect(() => {
    if (!enabled) return
    activeUsers += 1
    if (activeUsers === 1) {
      publish({ ...state, preferences: readPreferences() })
      void refreshPluginWebUI()
      registryTimer = window.setInterval(() => {
        void refreshPluginWebUI()
      }, 30000)
      unsubscribeBackend = onBackendUrlChanged(() => {
        generation += 1
        pending = null
        publish({ ...state, extensions: [], loading: true, error: null })
        void refreshPluginWebUI()
      })
    }
    return () => {
      activeUsers -= 1
      if (activeUsers === 0) {
        if (registryTimer !== null) clearInterval(registryTimer)
        registryTimer = null
        unsubscribeBackend?.()
        unsubscribeBackend = null
      }
    }
  }, [enabled])
  return snapshot
}

export function visibleExtensions(registry: RegistryState): WebUIExtension[] {
  const { hidden, order } = registry.preferences
  return registry.extensions
    .filter((extension) => !hidden.includes(extension.plugin_id))
    .sort((a, b) => {
      const rank = (id: string) => {
        const index = order.indexOf(id)
        return index === -1 ? order.length : index
      }
      return rank(a.plugin_id) - rank(b.plugin_id) || a.plugin_id.localeCompare(b.plugin_id)
    })
}

export async function invokePluginWebUI(
  pluginId: string,
  pageId: string,
  kind: 'queries' | 'actions',
  name: string,
  args: Record<string, Scalar>,
  confirmed = false,
  signal?: AbortSignal
): Promise<unknown> {
  const path = [pluginId, pageId, kind, name].map(encodeURIComponent).join('/')
  const response = await backendApi.post<{ result: unknown }>(
    `/api/webui/plugins/runtime/webui/${path}`,
    {
      body: { args, confirmed },
      signal,
    }
  )
  return response.result
}

export function resolveNodeValue(node: WebUINode, data: Record<string, unknown>): unknown {
  if (node.value === null || typeof node.value !== 'object') return node.value
  let value = data[node.value.source]
  for (const field of node.value.field ? node.value.field.split('.') : []) {
    if (value === null || typeof value !== 'object' || !Object.hasOwn(value, field)) {
      throw new Error(`Missing data field: ${node.value.source}.${node.value.field}`)
    }
    value = (value as Record<string, unknown>)[field]
  }
  return value
}
