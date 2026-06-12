import type { TFunction } from 'i18next'
import type { CSSProperties } from 'react'
import { Link } from '@tanstack/react-router'
import {
  Activity,
  AlertCircle,
  BarChart3,
  CheckCircle2,
  ClipboardCheck,
  Clock,
  Database,
  DollarSign,
  ExternalLink,
  FileText,
  HardDrive,
  ImageIcon,
  MessageSquare,
  Plus,
  Power,
  Puzzle,
  RefreshCw,
  RotateCcw,
  Settings,
  Smile,
  TrendingUp,
  Zap,
  type LucideIcon,
} from 'lucide-react'
import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  XAxis,
  YAxis,
} from 'recharts'

import { ExpressionReviewer } from '@/components/expression-reviewer'
import { RestartOverlay } from '@/components/restart-overlay'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from '@/components/ui/chart'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Progress } from '@/components/ui/progress'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ThinkingIllustration } from '@/components/ui/thinking-illustration'
import { ZoomableChart } from '@/components/ui/zoomable-chart'
import { getBotConfigCached, getModelConfigCached } from '@/lib/config-api'
import { getReviewStats } from '@/lib/expression-api'
import { backendApi } from '@/lib/http'
import {
  getInstalledPlugins,
  getPluginConfigSchema,
  type InstalledPlugin,
  type PluginConfigSchema,
} from '@/lib/plugin-api'
import { RestartProvider, useRestart } from '@/lib/restart-context'
import { getLocalCacheStats, type LocalCacheStats } from '@/lib/system-api'
import { ThemeProviderContext } from '@/lib/theme-context'
import type { DashboardStyle } from '@/lib/theme/tokens'
import { cn } from '@/lib/utils'
import { APP_VERSION } from '@/lib/version'

// 主导出组件：包装 RestartProvider
export function IndexPage() {
  return (
    <RestartProvider>
      <IndexPageContent />
    </RestartProvider>
  )
}

// 机器人状态接口
interface BotStatus {
  running: boolean
  uptime: number
  version: string
  start_time: string
}

interface ReleaseStatus {
  version: string
  url: string
}

interface StatisticsSummary {
  total_requests: number
  total_cost: number
  total_tokens: number
  online_time: number
  total_messages: number
  total_replies: number
  avg_response_time: number
  cost_per_hour: number
  tokens_per_hour: number
}

interface ModelStatistics {
  model_name: string
  request_count: number
  total_cost: number
  total_tokens: number
  avg_response_time: number
}

interface TimeSeriesData {
  timestamp: string
  requests: number
  cost: number
  tokens: number
}

interface RecentActivity {
  timestamp: string
  model: string
  request_type: string
  tokens: number
  cost: number
  time_cost: number
  status: string
}

interface DashboardData {
  summary: StatisticsSummary
  model_stats: ModelStatistics[]
  hourly_data: TimeSeriesData[]
  daily_data: TimeSeriesData[]
  recent_activity: RecentActivity[]
}

interface FeatureStatus {
  memoryEnabled: boolean
  visualEnabled: boolean
}

type QuickShortcutCategory = 'system' | 'config' | 'resource' | 'plugin' | 'monitor' | 'external'

interface QuickShortcutDefinition {
  id: string
  category: QuickShortcutCategory
  label: string
  description: string
  icon: LucideIcon
  href?: string
  action?: () => void | Promise<void>
  disabled?: boolean
  badge?: string
  external?: boolean
}

const DEFAULT_TIME_RANGE = 24
const DASHBOARD_DATA_CACHE_TTL = 5 * 60_000
const BOT_STATUS_CACHE_TTL = 30_000
const LOCAL_CACHE_STATS_CACHE_TTL = 15 * 60_000
const QUICK_SHORTCUT_STORAGE_KEY = 'maibot-home-quick-shortcuts'
const DEFAULT_QUICK_SHORTCUT_IDS = [
  'action:restart',
  'action:expression-review',
  'route:logs',
  'route:plugin-market',
  'route:settings',
  'external:statistics',
]
const dashboardDataCache = new Map<number, { timestamp: number; data: DashboardData }>()
let botStatusCache: { timestamp: number; data: BotStatus } | null = null
let localCacheStatsCache: { timestamp: number; data: LocalCacheStats } | null = null

function loadQuickShortcutIds(): string[] {
  const fallback = [...DEFAULT_QUICK_SHORTCUT_IDS]
  if (typeof window === 'undefined') {
    return fallback
  }

  const stored = localStorage.getItem(QUICK_SHORTCUT_STORAGE_KEY)
  if (!stored) {
    return fallback
  }

  try {
    const parsed = JSON.parse(stored)
    if (Array.isArray(parsed)) {
      const ids = parsed.filter((item): item is string => typeof item === 'string' && item.length > 0)
      return ids.length > 0 ? Array.from(new Set(ids)) : fallback
    }
  } catch {
    return fallback
  }

  return fallback
}

function saveQuickShortcutIds(ids: string[]): void {
  localStorage.setItem(QUICK_SHORTCUT_STORAGE_KEY, JSON.stringify(Array.from(new Set(ids))))
}

function getPluginShortcutId(pluginId: string, tabId?: string): string {
  const encodedPluginId = encodeURIComponent(pluginId)
  if (!tabId) {
    return `plugin-config:${encodedPluginId}`
  }
  return `plugin-config:${encodedPluginId}:tab:${encodeURIComponent(tabId)}`
}

function parsePluginShortcutId(id: string): { pluginId: string; tabId?: string } | null {
  if (!id.startsWith('plugin-config:')) {
    return null
  }

  const [, encodedPluginId, marker, encodedTabId] = id.split(':')
  if (!encodedPluginId) {
    return null
  }

  return {
    pluginId: decodeURIComponent(encodedPluginId),
    tabId: marker === 'tab' && encodedTabId ? decodeURIComponent(encodedTabId) : undefined,
  }
}

function getPluginConfigHref(pluginId: string, tabId?: string): string {
  const params = new URLSearchParams({ plugin: pluginId })
  if (tabId) {
    params.set('tab', tabId)
  }
  return `/plugin-config?${params.toString()}`
}

function buildBasePluginShortcut(plugin: InstalledPlugin, t: TFunction): QuickShortcutDefinition {
  const pluginName = plugin.manifest.name || plugin.id
  return {
    id: getPluginShortcutId(plugin.id),
    category: 'plugin',
    label: t('home.pluginShortcuts.baseLabel', { plugin: pluginName }),
    description: t('home.pluginShortcuts.baseDescription', { plugin: pluginName }),
    icon: Puzzle,
    href: getPluginConfigHref(plugin.id),
  }
}

async function loadPluginTabShortcuts(
  plugin: InstalledPlugin,
  t: TFunction,
  selectedTabIds?: Set<string>
): Promise<QuickShortcutDefinition[]> {
  const schemaResult = await getPluginConfigSchema(plugin.id)
  if (!schemaResult.success || !schemaResult.data) {
    return []
  }

  const pluginName = plugin.manifest.name || plugin.id
  const schema = schemaResult.data as PluginConfigSchema
  const schemaTabs = schema.layout.type === 'tabs' ? schema.layout.tabs : []
  const tabs = selectedTabIds ? schemaTabs.filter((tab) => selectedTabIds.has(tab.id)) : schemaTabs
  return tabs.map((tab) => ({
    id: getPluginShortcutId(plugin.id, tab.id),
    category: 'plugin' as const,
    label: `${pluginName} / ${tab.title || tab.id}`,
    description: t('home.pluginShortcuts.tabDescription', {
      plugin: pluginName,
      tab: tab.title || tab.id,
    }),
    icon: Puzzle,
    href: getPluginConfigHref(plugin.id, tab.id),
  }))
}

function getSelectedPluginTabIds(ids: string[]): Map<string, Set<string>> {
  const selectedTabs = new Map<string, Set<string>>()
  for (const id of ids) {
    const parsed = parsePluginShortcutId(id)
    if (!parsed?.tabId) {
      continue
    }

    const pluginTabs = selectedTabs.get(parsed.pluginId) ?? new Set<string>()
    pluginTabs.add(parsed.tabId)
    selectedTabs.set(parsed.pluginId, pluginTabs)
  }
  return selectedTabs
}

function getSelectedPluginIds(ids: string[]): Set<string> {
  const selectedPluginIds = new Set<string>()
  for (const id of ids) {
    const parsed = parsePluginShortcutId(id)
    if (parsed) {
      selectedPluginIds.add(parsed.pluginId)
    }
  }
  return selectedPluginIds
}

function getFallbackPluginShortcut(id: string, t: TFunction): QuickShortcutDefinition | null {
  const parsed = parsePluginShortcutId(id)
  if (!parsed) {
    return null
  }

  return {
    id,
    category: 'plugin',
    label: parsed.tabId
      ? t('home.pluginShortcuts.fallbackTabLabel', { plugin: parsed.pluginId, tab: parsed.tabId })
      : t('home.pluginShortcuts.fallbackLabel', { plugin: parsed.pluginId }),
    description: parsed.tabId
      ? t('home.pluginShortcuts.fallbackTabDescription')
      : t('home.pluginShortcuts.fallbackDescription'),
    icon: Puzzle,
    href: getPluginConfigHref(parsed.pluginId, parsed.tabId),
  }
}

function getCachedDashboardData(hours: number): DashboardData | null {
  const cached = dashboardDataCache.get(hours)
  if (!cached || Date.now() - cached.timestamp > DASHBOARD_DATA_CACHE_TTL) {
    return null
  }
  return cached.data
}

function getStaleDashboardData(hours: number): DashboardData | null {
  return dashboardDataCache.get(hours)?.data ?? null
}

function getCachedBotStatus(): BotStatus | null {
  if (!botStatusCache || Date.now() - botStatusCache.timestamp > BOT_STATUS_CACHE_TTL) {
    return null
  }
  return botStatusCache.data
}

function getCachedLocalCacheStats(): LocalCacheStats | null {
  if (!localCacheStatsCache || Date.now() - localCacheStatsCache.timestamp > LOCAL_CACHE_STATS_CACHE_TTL) {
    return null
  }
  return localCacheStatsCache.data
}

const FUTURE_RETRO_PIE_COLORS = [
  '#0b5a66',
  '#c84d24',
  '#8b6f2a',
  '#2f7d6f',
  '#9b3f58',
  '#57704a',
  '#284b63',
  '#d08a2d',
  '#6b5b95',
  '#7a4f2b',
]

// 为饼图生成颜色；未来复古模式使用更贴近纸张、青绿边框和橘红强调色的调色盘。
const generatePieColors = (count: number, dashboardStyle: DashboardStyle): string[] => {
  if (dashboardStyle === 'future-retro') {
    return Array.from({ length: count }, (_, index) => FUTURE_RETRO_PIE_COLORS[index % FUTURE_RETRO_PIE_COLORS.length])
  }

  const colors: string[] = []
  for (let i = 0; i < count; i++) {
    // 使用黄金角度分布色相，避免相邻颜色相似
    const hue = (i * 137.508) % 360
    colors.push(`hsl(${hue}, 70%, 55%)`)
  }
  return colors
}

// 内部实现组件
function FeatureStatusIndicator({
  accent,
  detail,
  enabled,
  label,
}: {
  accent: 'green' | 'orange' | 'yellow' | 'red'
  detail?: string
  enabled: boolean
  label: string
}) {
  const enabledColorClass = {
    green: 'text-green-600',
    orange: 'text-orange-600',
    yellow: 'text-yellow-600',
    red: 'text-red-600',
  }[accent]
  const enabledBarClass = {
    green: 'bg-green-500',
    orange: 'bg-orange-500',
    yellow: 'bg-yellow-400',
    red: 'bg-red-500',
  }[accent]

  return (
    <div
      data-dashboard-feature-status="true"
      data-accent={accent}
      data-enabled={enabled ? 'true' : 'false'}
      className={cn(
        'flex min-h-9 w-full items-center gap-2.5 px-1 py-1 font-sans text-base font-bold transition-colors',
        enabled ? enabledColorClass : 'text-muted-foreground/55'
      )}
    >
      <span
        data-dashboard-feature-status-bar="true"
        className={cn(
          'h-8 w-2.5 shrink-0 rounded-[2px] transition-colors',
          enabled ? enabledBarClass : 'bg-muted-foreground/25'
        )}
      />
      <span className="min-w-0 flex-1 truncate">
        {label}
        {detail && <span className="ml-2 text-sm font-semibold opacity-75">· {detail}</span>}
      </span>
    </div>
  )
}

function FeatureStatusLight({ enabled, label }: { enabled: boolean; label: string }) {
  return (
    <div
      data-dashboard-feature-status="true"
      data-enabled={enabled ? 'true' : 'false'}
      className="inline-flex items-center gap-1.5 rounded-md border bg-background px-2 py-1 text-xs text-muted-foreground"
    >
      <span
        data-dashboard-feature-status-light="true"
        className={cn(
          'h-2.5 w-2.5 rounded-full',
          enabled ? 'bg-green-500 shadow-[0_0_0_3px_rgba(34,197,94,0.18)]' : 'bg-muted-foreground/30'
        )}
      />
      <span>{label}</span>
    </div>
  )
}

function formatStorageBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return '0 B'
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const unitIndex = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const value = bytes / 1024 ** unitIndex
  return `${value.toFixed(value >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`
}

function IndexPageContent() {
  const { t, i18n } = useTranslation()
  const { themeConfig } = useContext(ThemeProviderContext)
  const currentLocale = i18n.resolvedLanguage || i18n.language || 'zh-CN'
  const initialDashboardData = getCachedDashboardData(DEFAULT_TIME_RANGE) ?? getStaleDashboardData(DEFAULT_TIME_RANGE)
  const [dashboardData, setDashboardData] = useState<DashboardData | null>(initialDashboardData)
  const [loading, setLoading] = useState(!initialDashboardData)
  const [loadingProgress, setLoadingProgress] = useState(0)
  const [timeRange, setTimeRange] = useState(DEFAULT_TIME_RANGE) // 默认24小时
  const [hitokoto, setHitokoto] = useState<{ hitokoto: string; from: string } | null>(null)
  const [hitokotoLoading, setHitokotoLoading] = useState(true)
  const [botStatus, setBotStatus] = useState<BotStatus | null>(botStatusCache?.data ?? null)
  const [isBotStatusLoading, setIsBotStatusLoading] = useState(!botStatusCache)
  const [maibotStableRelease, setMaibotStableRelease] = useState<ReleaseStatus | null>(null)
  const [featureStatus, setFeatureStatus] = useState<FeatureStatus>({
    memoryEnabled: false,
    visualEnabled: false,
  })
  const [localCacheStats, setLocalCacheStats] = useState<LocalCacheStats | null>(localCacheStatsCache?.data ?? null)
  const [isLocalCacheStatsLoading, setIsLocalCacheStatsLoading] = useState(!localCacheStatsCache)
  const [isReviewerOpen, setIsReviewerOpen] = useState(false)
  const [uncheckedCount, setUncheckedCount] = useState(0)
  const [quickShortcutIds, setQuickShortcutIds] = useState<string[]>(loadQuickShortcutIds)
  const [quickShortcutDialogOpen, setQuickShortcutDialogOpen] = useState(false)
  const [quickShortcutSearch, setQuickShortcutSearch] = useState('')
  const [pluginShortcuts, setPluginShortcuts] = useState<QuickShortcutDefinition[]>([])
  const [isPluginShortcutsLoading, setIsPluginShortcutsLoading] = useState(false)
  const { triggerRestart, isRestarting } = useRestart()
  
  // 使用 ref 跟踪组件是否已卸载，防止内存泄漏
  const isMountedRef = useRef(true)

  // 组件卸载时清理
  useEffect(() => {
    isMountedRef.current = true
    return () => {
      isMountedRef.current = false
    }
  }, [])

  useEffect(() => {
    let mounted = true

    const loadLatestVersions = async () => {
      try {
        const response = await fetch('https://api.github.com/repos/Mai-with-u/MaiBot/releases?per_page=20', {
          headers: { Accept: 'application/vnd.github+json' },
        })
        if (!response.ok) {
          throw new Error(`GitHub release status ${response.status}`)
        }
        const releases = await response.json() as Array<{
          draft?: boolean
          prerelease?: boolean
          tag_name?: string
          html_url?: string
        }>
        const visibleReleases = releases.filter((release) => !release.draft)
        const stableRelease = visibleReleases.find((release) => !release.prerelease)
        if (mounted) {
          if (stableRelease?.tag_name) {
            setMaibotStableRelease({
              version: String(stableRelease.tag_name).replace(/^v/i, '').trim(),
              url: stableRelease.html_url || 'https://github.com/Mai-with-u/MaiBot/releases',
            })
          }
        }
      } catch (error) {
        console.debug('检查 MaiBot 最新版本失败:', error)
      }

    }

    void loadLatestVersions()

    return () => {
      mounted = false
    }
  }, [])

  // 获取审核统计
  const fetchReviewStats = useCallback(async () => {
    try {
      const result = await getReviewStats()
      if (result.success && isMountedRef.current) {
        setUncheckedCount(result.data.unchecked)
      }
    } catch (error) {
      console.error('获取审核统计失败:', error)
    }
  }, [])

  // 获取一言
  const fetchHitokoto = useCallback(async () => {
    try {
      setHitokotoLoading(true)
      const response = await fetch('https://v1.hitokoto.cn/?c=a&c=b&c=c&c=d&c=h&c=i&c=k')
      if (!response.ok) {
        throw new Error(`一言接口返回 HTTP ${response.status}`)
      }
      const data = await response.json()
      if (isMountedRef.current) {
        setHitokoto({
          hitokoto: data.hitokoto,
          from: data.from || data.from_who || t('home.unknownSource')
        })
      }
    } catch (error) {
      console.error('获取一言失败:', error)
      if (isMountedRef.current) {
        setHitokoto({
          hitokoto: t('home.hitokotoFallback'),
          from: t('home.hitokotoFallbackFrom')
        })
      }
    } finally {
      if (isMountedRef.current) {
        setHitokotoLoading(false)
      }
    }
  }, [t])

  // 获取机器人状态
  const fetchBotStatus = useCallback(async (force = false) => {
    const cachedStatus = force ? null : getCachedBotStatus()
    if (cachedStatus) {
      setBotStatus(cachedStatus)
      setIsBotStatusLoading(false)
      return
    }

    setIsBotStatusLoading(true)
    try {
      const data = await backendApi.get<BotStatus>('/api/webui/system/status')
      if (!isMountedRef.current) return
      botStatusCache = { timestamp: Date.now(), data }
      setBotStatus(data)
    } catch (error) {
      console.error('获取机器人状态失败:', error)
      if (isMountedRef.current && !botStatusCache) {
        setBotStatus(null)
      }
    } finally {
      if (isMountedRef.current) {
        setIsBotStatusLoading(false)
      }
    }
  }, [])

  // 重启机器人
  const fetchFeatureStatus = useCallback(async () => {
    try {
      const [botConfigResult, modelConfigResult] = await Promise.all([
        getBotConfigCached(),
        getModelConfigCached(),
      ])

      if (!isMountedRef.current || !botConfigResult.success) return

      const botPayload = botConfigResult.data as { config?: Record<string, unknown> } & Record<string, unknown>
      const botConfig = (botPayload.config ?? botPayload) as Record<string, unknown>
      const memorixConfig = (botConfig.a_memorix ?? {}) as Record<string, unknown>
      const memorixPlugin = (memorixConfig.plugin ?? {}) as Record<string, unknown>

      const modelPayload = modelConfigResult.success
        ? (modelConfigResult.data as { config?: Record<string, unknown> } & Record<string, unknown>)
        : {}
      const modelConfig = (modelPayload.config ?? modelPayload) as Record<string, unknown>
      const taskConfig = (modelConfig.model_task_config ?? {}) as Record<string, unknown>
      const vlmTask = (taskConfig.vlm ?? {}) as Record<string, unknown>
      const vlmModelList = Array.isArray(vlmTask.model_list) ? vlmTask.model_list : []
      const hasVlmModel = vlmModelList.some((modelName) => String(modelName ?? '').trim().length > 0)

      setFeatureStatus({
        memoryEnabled: memorixPlugin.enabled === true,
        visualEnabled: hasVlmModel,
      })
    } catch (error) {
      console.error('获取功能启用状态失败:', error)
      if (isMountedRef.current) {
        setFeatureStatus({
          memoryEnabled: false,
          visualEnabled: false,
        })
      }
    }
  }, [])

  const fetchLocalCacheStats = useCallback(async () => {
    const cachedStats = getCachedLocalCacheStats()
    if (cachedStats) {
      setLocalCacheStats(cachedStats)
      setIsLocalCacheStatsLoading(false)
      return
    }

    setIsLocalCacheStatsLoading(true)
    try {
      const stats = await getLocalCacheStats()
      if (isMountedRef.current) {
        localCacheStatsCache = { timestamp: Date.now(), data: stats }
        setLocalCacheStats(stats)
      }
    } catch (error) {
      console.error('获取本地存储占用失败:', error)
      if (isMountedRef.current && !localCacheStatsCache) {
        setLocalCacheStats(null)
      }
    } finally {
      if (isMountedRef.current) {
        setIsLocalCacheStatsLoading(false)
      }
    }
  }, [])

  const handleRestart = useCallback(async () => {
    await triggerRestart()
  }, [triggerRestart])

  useEffect(() => {
    let cancelled = false

    const loadPluginShortcuts = async () => {
      const selectedPluginIds = getSelectedPluginIds(quickShortcutIds)
      const selectedPluginTabIds = getSelectedPluginTabIds(quickShortcutIds)
      if (!quickShortcutDialogOpen && selectedPluginIds.size === 0) {
        setPluginShortcuts([])
        setIsPluginShortcutsLoading(false)
        return
      }

      setIsPluginShortcutsLoading(true)
      try {
        const installedResult = await getInstalledPlugins()
        if (!installedResult.success || cancelled) {
          return
        }

        const enabledPlugins = installedResult.data
          .filter((plugin) => plugin.disabled !== true && plugin.enabled !== false)
          .filter((plugin, index, all) => index === all.findIndex((item) => item.id === plugin.id))

        const visiblePlugins = quickShortcutDialogOpen
          ? enabledPlugins
          : enabledPlugins.filter((plugin) => selectedPluginIds.has(plugin.id))
        const baseShortcuts = visiblePlugins.map((plugin) => buildBasePluginShortcut(plugin, t))
        if (!cancelled) {
          setPluginShortcuts(baseShortcuts)
        }

        if (selectedPluginTabIds.size === 0) {
          return
        }

        const enabledPluginMap = new Map(enabledPlugins.map((plugin) => [plugin.id, plugin]))
        const tabShortcuts = (
          await Promise.all(
            Array.from(selectedPluginTabIds.entries()).map(async ([pluginId, selectedTabIds]) => {
              const plugin = enabledPluginMap.get(pluginId)
              if (!plugin) {
                return []
              }

              try {
                return await loadPluginTabShortcuts(plugin, t, selectedTabIds)
              } catch (error) {
                console.warn(`加载插件 ${plugin.id} 已选配置页签快捷入口失败:`, error)
                return []
              }
            })
          )
        ).flat()

        if (!cancelled) {
          setPluginShortcuts([...baseShortcuts, ...tabShortcuts])
        }
      } catch (error) {
        console.error('加载插件快捷入口失败:', error)
      } finally {
        if (!cancelled) {
          setIsPluginShortcutsLoading(false)
        }
      }
    }

    void loadPluginShortcuts()

    return () => {
      cancelled = true
    }
  }, [quickShortcutDialogOpen, quickShortcutIds, t])

  const quickShortcutOptions = useMemo<QuickShortcutDefinition[]>(
    () => [
      {
        id: 'action:restart',
        category: 'system',
        label: isRestarting ? t('home.quickActions.restarting') : t('home.quickActions.restart'),
        description: t('home.quickActions.descriptions.restart'),
        icon: RotateCcw,
        action: handleRestart,
        disabled: isRestarting,
      },
      {
        id: 'action:expression-review',
        category: 'resource',
        label: t('home.quickActions.expressionReview'),
        description: t('home.quickActions.descriptions.expressionReview'),
        icon: ClipboardCheck,
        action: () => setIsReviewerOpen(true),
        badge: uncheckedCount > 0 ? (uncheckedCount > 99 ? '99+' : String(uncheckedCount)) : undefined,
      },
      {
        id: 'route:logs',
        category: 'monitor',
        label: t('home.quickActions.viewLogs'),
        description: t('home.quickActions.descriptions.viewLogs'),
        icon: FileText,
        href: '/logs',
      },
      {
        id: 'route:plugin-market',
        category: 'plugin',
        label: t('home.quickActions.pluginManage'),
        description: t('home.quickActions.descriptions.pluginManage'),
        icon: Puzzle,
        href: '/plugins',
      },
      {
        id: 'route:plugin-config',
        category: 'plugin',
        label: t('home.quickActions.pluginConfig'),
        description: t('home.quickActions.descriptions.pluginConfig'),
        icon: Settings,
        href: '/plugin-config',
      },
      {
        id: 'route:settings',
        category: 'system',
        label: t('home.quickActions.systemSettings'),
        description: t('home.quickActions.descriptions.systemSettings'),
        icon: Settings,
        href: '/settings',
      },
      {
        id: 'route:settings-appearance',
        category: 'system',
        label: t('home.quickActions.appearanceSettings'),
        description: t('home.quickActions.descriptions.appearanceSettings'),
        icon: Settings,
        href: '/settings?tab=appearance',
      },
      {
        id: 'route:settings-local-cache',
        category: 'system',
        label: t('home.quickActions.localCache'),
        description: t('home.quickActions.descriptions.localCache'),
        icon: HardDrive,
        href: '/settings?tab=local-cache',
      },
      {
        id: 'route:model-providers',
        category: 'config',
        label: t('home.quickActions.modelProviders'),
        description: t('home.quickActions.descriptions.modelProviders'),
        icon: Settings,
        href: '/config/model?tab=providers',
      },
      {
        id: 'route:model-list',
        category: 'config',
        label: t('home.quickActions.modelList'),
        description: t('home.quickActions.descriptions.modelList'),
        icon: Settings,
        href: '/config/model?tab=models',
      },
      {
        id: 'route:model-tasks',
        category: 'config',
        label: t('home.quickActions.modelTasks'),
        description: t('home.quickActions.descriptions.modelTasks'),
        icon: Settings,
        href: '/config/model?tab=tasks',
      },
      {
        id: 'route:bot-config',
        category: 'config',
        label: t('home.quickActions.botConfig'),
        description: t('home.quickActions.descriptions.botConfig'),
        icon: Settings,
        href: '/config/bot',
      },
      {
        id: 'route:emoji',
        category: 'resource',
        label: t('home.quickActions.emojiManagement'),
        description: t('home.quickActions.descriptions.emojiManagement'),
        icon: MessageSquare,
        href: '/resource/emoji',
      },
      {
        id: 'route:expression',
        category: 'resource',
        label: t('home.quickActions.expressionManagement'),
        description: t('home.quickActions.descriptions.expressionManagement'),
        icon: MessageSquare,
        href: '/resource/expression',
      },
      {
        id: 'external:statistics',
        category: 'external',
        label: t('home.quickActions.statistics'),
        description: t('home.quickActions.descriptions.statistics'),
        icon: BarChart3,
        href: '/maibot_statistics.html',
        external: true,
      },
      ...pluginShortcuts,
    ],
    [handleRestart, isRestarting, pluginShortcuts, t, uncheckedCount]
  )

  const quickShortcutMap = useMemo(
    () => new Map(quickShortcutOptions.map((shortcut) => [shortcut.id, shortcut])),
    [quickShortcutOptions]
  )

  const selectedQuickShortcuts = useMemo(
    () =>
      quickShortcutIds
        .map((id) => quickShortcutMap.get(id) ?? getFallbackPluginShortcut(id, t))
        .filter((shortcut): shortcut is QuickShortcutDefinition => Boolean(shortcut)),
    [quickShortcutIds, quickShortcutMap, t]
  )

  const filteredQuickShortcutOptions = useMemo(() => {
    const query = quickShortcutSearch.trim().toLowerCase()
    if (!query) {
      return quickShortcutOptions
    }

    return quickShortcutOptions.filter((shortcut) =>
      `${shortcut.label} ${shortcut.description}`.toLowerCase().includes(query)
    )
  }, [quickShortcutOptions, quickShortcutSearch])

  const updateQuickShortcutIds = useCallback((nextIds: string[]) => {
    const normalizedIds = Array.from(new Set(nextIds))
    setQuickShortcutIds(normalizedIds)
    saveQuickShortcutIds(normalizedIds)
  }, [])

  const toggleQuickShortcut = useCallback(
    (id: string, checked: boolean) => {
      updateQuickShortcutIds(
        checked ? [...quickShortcutIds, id] : quickShortcutIds.filter((shortcutId) => shortcutId !== id)
      )
    },
    [quickShortcutIds, updateQuickShortcutIds]
  )

  const resetQuickShortcuts = useCallback(() => {
    updateQuickShortcutIds([...DEFAULT_QUICK_SHORTCUT_IDS])
  }, [updateQuickShortcutIds])

  const fetchDashboardData = useCallback(async (force = false) => {
    try {
      const cachedData = force ? null : getCachedDashboardData(timeRange)
      if (cachedData) {
        setDashboardData(cachedData)
        setLoading(false)
        setLoadingProgress(100)
        return
      }

      const staleData = getStaleDashboardData(timeRange)
      if (staleData) {
        setDashboardData(staleData)
        setLoading(false)
        setLoadingProgress(100)
      } else {
        setLoading(true)
      }
      const data = await backendApi.get<DashboardData>('/api/webui/statistics/dashboard', {
        query: { hours: timeRange },
      })
      if (!isMountedRef.current) return
      dashboardDataCache.set(timeRange, { timestamp: Date.now(), data })
      setDashboardData(data)
      setLoading(false)
      setLoadingProgress(100)
    } catch (error) {
      console.error('Failed to fetch dashboard data:', error)
      if (isMountedRef.current) {
        setLoading(false)
        setLoadingProgress(100)
      }
    }
  }, [timeRange])

  // 伪加载进度条效果
  useEffect(() => {
    if (!loading) return

    setLoadingProgress(0)
    
    // 快速到15%
    const timer1 = setTimeout(() => setLoadingProgress(15), 200)
    // 到30%
    const timer2 = setTimeout(() => setLoadingProgress(30), 800)
    // 到45%
    const timer3 = setTimeout(() => setLoadingProgress(45), 2000)
    // 到60%
    const timer4 = setTimeout(() => setLoadingProgress(60), 4000)
    // 到75%
    const timer5 = setTimeout(() => setLoadingProgress(75), 6500)
    // 到85%
    const timer6 = setTimeout(() => setLoadingProgress(85), 9000)
    // 到92%
    const timer7 = setTimeout(() => setLoadingProgress(92), 11000)

    return () => {
      clearTimeout(timer1)
      clearTimeout(timer2)
      clearTimeout(timer3)
      clearTimeout(timer4)
      clearTimeout(timer5)
      clearTimeout(timer6)
      clearTimeout(timer7)
    }
  }, [loading])

  useEffect(() => {
    fetchDashboardData()
    fetchHitokoto()
    fetchBotStatus(true)
    fetchFeatureStatus()
    fetchLocalCacheStats()
    fetchReviewStats()
  }, [fetchDashboardData, fetchHitokoto, fetchBotStatus, fetchFeatureStatus, fetchLocalCacheStats, fetchReviewStats])

  useEffect(() => {
    const refreshBotStatus = () => {
      if (isMountedRef.current) {
        fetchBotStatus(true)
      }
    }

    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        refreshBotStatus()
      }
    }

    const intervalId = setInterval(refreshBotStatus, 30000)
    document.addEventListener('visibilitychange', handleVisibilityChange)
    window.addEventListener('focus', refreshBotStatus)

    return () => {
      clearInterval(intervalId)
      document.removeEventListener('visibilitychange', handleVisibilityChange)
      window.removeEventListener('focus', refreshBotStatus)
    }
  }, [fetchBotStatus])

  if (loading || !dashboardData) {
    return (
      <div className="flex items-center justify-center h-[calc(100vh-200px)]">
        <div className="text-center space-y-6 w-full max-w-md px-4">
          <ThinkingIllustration size="lg" className="mx-auto" />
          <div className="space-y-2">
            <Progress value={loadingProgress} className="h-2" />
            <p className="text-xs text-muted-foreground">{loadingProgress}%</p>
          </div>
        </div>
      </div>
    )
  }

  // 解构数据，提供默认值以防止 undefined 错误
  const { 
    summary: rawSummary, 
    model_stats = [], 
    hourly_data = [], 
    daily_data = [], 
    recent_activity = [] 
  } = dashboardData

  // 为 summary 提供默认值
  const summary = rawSummary ?? {
    total_requests: 0,
    total_cost: 0,
    total_tokens: 0,
    online_time: 0,
    total_messages: 0,
    total_replies: 0,
    avg_response_time: 0,
    cost_per_hour: 0,
    tokens_per_hour: 0,
  }

  // 格式化时间显示
  const formatTime = (seconds: number) => {
    const hours = Math.floor(seconds / 3600)
    const minutes = Math.floor((seconds % 3600) / 60)
    return t('home.time.hoursMinutes', { hours, minutes })
  }

  // 格式化大数字（自动选择合适单位）
  const formatNumber = (num: number): { display: string; exact: string; needsExact: boolean } => {
    const exact = num.toLocaleString(currentLocale)
    
    if (num >= 1_000_000_000) {
      return { display: `${(num / 1_000_000_000).toFixed(2)}B`, exact, needsExact: true }
    } else if (num >= 1_000_000) {
      return { display: `${(num / 1_000_000).toFixed(2)}M`, exact, needsExact: true }
    } else if (num >= 10_000) {
      return { display: `${(num / 1_000).toFixed(1)}K`, exact, needsExact: true }
    } else if (num >= 1_000) {
      return { display: `${(num / 1_000).toFixed(2)}K`, exact, needsExact: true }
    }
    return { display: exact, exact, needsExact: false }
  }

  // 格式化金额（自动选择合适单位）
  const formatCurrency = (num: number): { display: string; exact: string; needsExact: boolean } => {
    const exact = `¥${num.toLocaleString(currentLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
    
    if (num >= 1_000_000) {
      return { display: `¥${(num / 1_000_000).toFixed(2)}M`, exact, needsExact: true }
    } else if (num >= 10_000) {
      return { display: `¥${(num / 1_000).toFixed(1)}K`, exact, needsExact: true }
    } else if (num >= 1_000) {
      return { display: `¥${(num / 1_000).toFixed(2)}K`, exact, needsExact: true }
    }
    return { display: exact, exact, needsExact: false }
  }

  // 格式化日期时间
  const formatDateTime = (isoString: string) => {
    const date = new Date(isoString)
    return date.toLocaleString(currentLocale, {
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  // 准备饼图数据（模型花费分布）- 使用黄金角度分布避免相邻颜色相似
  const pieColors = generatePieColors(model_stats.length, themeConfig.dashboardStyle)
  const modelPieData = model_stats.map((stat, index) => ({
    name: stat.model_name,
    value: stat.total_cost,
    fill: pieColors[index],
  }))

  // 图表配置
  const chartConfig = {
    requests: {
      label: t('home.charts.requests'),
      color: 'hsl(var(--color-chart-1))',
    },
    cost: {
      label: t('home.charts.cost'),
      color: 'hsl(var(--color-chart-2))',
    },
    tokens: {
      label: 'Tokens',
      color: 'hsl(var(--color-chart-3))',
    },
  } satisfies ChartConfig

  const localCacheDirectories = localCacheStats?.directories ?? []
  const imageCacheDirectory = localCacheDirectories.find((item) => item.key === 'images')
  const emojiCacheDirectory = localCacheDirectories.find((item) => item.key === 'emoji')
  const logCacheDirectory = localCacheDirectories.find((item) => item.key === 'logs')
  const imageCacheSize = imageCacheDirectory?.total_size ?? 0
  const emojiCacheSize = emojiCacheDirectory?.total_size ?? 0
  const logCacheSize = logCacheDirectory?.total_size ?? 0
  const databaseSize = localCacheStats?.database.total_size ?? 0
  const totalStorageSize = localCacheDirectories.reduce((total, item) => total + item.total_size, 0) + databaseSize
  const hasLocalCacheStats = localCacheStats !== null
  const storageDetails = [
    {
      key: 'images',
      label: t('home.storage.images'),
      size: imageCacheSize,
      detail: t('home.storage.files', { count: imageCacheDirectory?.file_count ?? 0 }),
      icon: ImageIcon,
    },
    {
      key: 'emoji',
      label: t('home.storage.emoji'),
      size: emojiCacheSize,
      detail: t('home.storage.filesAndRecords', {
        files: emojiCacheDirectory?.file_count ?? 0,
        records: emojiCacheDirectory?.db_records ?? 0,
      }),
      icon: Smile,
    },
    {
      key: 'logs',
      label: t('home.storage.logs'),
      size: logCacheSize,
      detail: t('home.storage.files', { count: logCacheDirectory?.file_count ?? 0 }),
      icon: FileText,
    },
    {
      key: 'database',
      label: t('home.storage.database'),
      size: databaseSize,
      detail: t('home.storage.databaseDetail', {
        files: localCacheStats?.database.files.length ?? 0,
        tables: localCacheStats?.database.tables.length ?? 0,
      }),
      icon: Database,
    },
  ]

  return (
    <ScrollArea className="h-full">
      <div className="space-y-2 sm:space-y-4 p-4 sm:p-6">
      {/* 标题和控制栏 */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl sm:text-3xl font-bold">{t('home.title')}</h1>
        </div>
      </div>

      {/* 一言 */}
      <div
        className={cn(
          'flex items-center gap-3 rounded-lg bg-muted/20 px-4 py-1',
          themeConfig.dashboardStyle !== 'future-retro' && 'border border-dashed border-muted-foreground/30'
        )}
      >
        {hitokotoLoading ? (
          <Skeleton className="h-5 flex-1" />
        ) : hitokoto ? (
          <p
            className={cn(
              'flex-1 truncate text-muted-foreground',
              themeConfig.dashboardStyle === 'future-retro'
                ? 'text-[1.05rem] font-medium tracking-wide'
                : 'text-sm italic'
            )}
            style={
              themeConfig.dashboardStyle === 'future-retro'
                ? {
                    fontFamily:
                      '"Cormorant Garamond", "EB Garamond", "Libre Baskerville", "Baskerville", "Palatino Linotype", "Book Antiqua", "Noto Serif SC", "Source Han Serif SC", "Songti SC", "STSong", "SimSun", serif',
                    textShadow: '0 0.035em 0 hsl(var(--background))',
                  }
                : undefined
            }
          >
            "{hitokoto.hitokoto}" —— {hitokoto.from}
          </p>
        ) : null}
      </div>

      {/* 机器人状态和快速操作 */}
      <div
        data-home-summary-cards="true"
        className="grid items-stretch gap-4 grid-cols-1 lg:grid-cols-[minmax(14rem,0.8fr)_minmax(16rem,1fr)_minmax(0,1.6fr)]"
      >
        {/* 机器人状态卡片 */}
        <Card className="lg:col-span-1">
          <CardHeader className="pb-3">
            <CardTitle className="flex h-5 items-center gap-2 text-sm font-medium leading-5">
              <FileText className="h-4 w-4" />
              {t('home.versionCard.title')}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              <div className="flex items-center justify-between gap-3">
                <span className="text-sm text-muted-foreground">{t('home.versionCard.mainVersion')}</span>
                <Badge
                  variant="secondary"
                  data-dashboard-version-value="true"
                  className="border border-primary/20 bg-primary/10 px-2 py-0.5 font-semibold text-primary"
                >
                  {botStatus?.version ? `v${botStatus.version}` : t('home.versionCard.unknown')}
                </Badge>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-sm text-muted-foreground">{t('home.versionCard.webuiVersion')}</span>
                <Badge
                  variant="secondary"
                  data-dashboard-version-value="true"
                  className="border border-primary/20 bg-primary/10 px-2 py-0.5 font-semibold text-primary"
                >
                  v{APP_VERSION}
                </Badge>
              </div>
              <div className="space-y-1 border-t border-border/50 pt-2 text-xs text-muted-foreground/60">
                <a
                  href={maibotStableRelease?.url || 'https://github.com/Mai-with-u/MaiBot/releases'}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center justify-between gap-2 transition-colors hover:text-muted-foreground"
                >
                  <span>{t('home.versionCard.stableLatest')}</span>
                  <span className="inline-flex items-center gap-1">
                    {maibotStableRelease ? `v${maibotStableRelease.version}` : t('home.versionCard.githubReleases')}
                    <ExternalLink className="h-3 w-3" />
                  </span>
                </a>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="lg:col-span-1">
          <CardHeader className="pb-3">
            <CardTitle className="flex h-5 items-center gap-2 text-sm font-medium leading-5">
              <Power className="h-4 w-4" />
              {t('home.botStatus.title')}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {themeConfig.dashboardStyle === 'future-retro' ? (
                <div className="space-y-2">
                  {isBotStatusLoading && !botStatus ? (
                    <FeatureStatusIndicator enabled={false} accent="green" label={t('home.botStatus.loading')} />
                  ) : botStatus?.running ? (
                    <FeatureStatusIndicator
                      enabled
                      accent="green"
                      label={t('home.botStatus.running')}
                      detail={t('home.botStatus.uptime', { time: formatTime(botStatus.uptime) })}
                    />
                  ) : botStatus ? (
                    <FeatureStatusIndicator enabled accent="red" label={t('home.botStatus.stopped')} />
                  ) : (
                    <FeatureStatusIndicator enabled={false} accent="green" label={t('home.botStatus.unknown')} />
                  )}
                  <FeatureStatusIndicator
                    accent="orange"
                    enabled={featureStatus.visualEnabled}
                    label={t('home.botStatus.visualEnabled')}
                  />
                  <FeatureStatusIndicator
                    accent="yellow"
                    enabled={featureStatus.memoryEnabled}
                    label={t('home.botStatus.memoryEnabled')}
                  />
                </div>
              ) : (
                <>
                  <div className="flex flex-wrap items-center gap-4">
                    <div className="flex items-center gap-2">
                      {isBotStatusLoading && !botStatus ? (
                        <>
                          <div
                            data-dashboard-status-dot="true"
                            data-state="loading"
                            className="h-3 w-3 rounded-full bg-muted-foreground/40 animate-pulse"
                          />
                          <Badge
                            data-dashboard-status-badge="true"
                            data-state="loading"
                            variant="outline"
                            className="whitespace-nowrap text-muted-foreground"
                          >
                            <RefreshCw className="h-3 w-3 mr-1 animate-spin" />
                            {t('home.botStatus.loading')}
                          </Badge>
                        </>
                      ) : botStatus?.running ? (
                        <>
                          <div
                            data-dashboard-status-dot="true"
                            data-state="running"
                            className="h-3 w-3 rounded-full bg-green-500 animate-pulse"
                          />
                          <Badge
                            data-dashboard-status-badge="true"
                            data-state="running"
                            variant="outline"
                            className="whitespace-nowrap text-green-600 border-green-300 bg-green-50"
                          >
                            <CheckCircle2 className="h-3 w-3 mr-1" />
                            {t('home.botStatus.running')}
                          </Badge>
                        </>
                      ) : botStatus ? (
                        <>
                          <div
                            data-dashboard-status-dot="true"
                            data-state="stopped"
                            className="h-3 w-3 rounded-full bg-red-500"
                          />
                          <Badge
                            data-dashboard-status-badge="true"
                            data-state="stopped"
                            variant="outline"
                            className="whitespace-nowrap text-red-600 border-red-300 bg-red-50"
                          >
                            <AlertCircle className="h-3 w-3 mr-1" />
                            {t('home.botStatus.stopped')}
                          </Badge>
                        </>
                      ) : (
                        <>
                          <div
                            data-dashboard-status-dot="true"
                            data-state="unknown"
                            className="h-3 w-3 rounded-full bg-muted-foreground/40"
                          />
                          <Badge
                            data-dashboard-status-badge="true"
                            data-state="unknown"
                            variant="outline"
                            className="whitespace-nowrap text-muted-foreground"
                          >
                            <AlertCircle className="h-3 w-3 mr-1" />
                            {t('home.botStatus.unknown')}
                          </Badge>
                        </>
                      )}
                    </div>
                    {botStatus && (
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <span>{t('home.botStatus.uptime', { time: formatTime(botStatus.uptime) })}</span>
                      </div>
                    )}
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <FeatureStatusLight enabled={featureStatus.visualEnabled} label={t('home.botStatus.visualEnabled')} />
                    <FeatureStatusLight enabled={featureStatus.memoryEnabled} label={t('home.botStatus.memoryEnabled')} />
                  </div>
                </>
              )}
            </div>
          </CardContent>
        </Card>

        {/* 快速操作卡片 */}
        <Card>
          <CardHeader className="flex flex-row items-start justify-between space-y-0 pb-3">
            <CardTitle className="flex h-5 items-center gap-2 text-sm font-medium leading-5">
              <Zap className="h-4 w-4" />
              {t('home.quickActions.title')}
            </CardTitle>
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setQuickShortcutDialogOpen(true)}
              aria-label={t('home.quickActions.customize')}
              className="h-8 w-8"
            >
              <Plus className="h-4 w-4" />
            </Button>
          </CardHeader>
          <CardContent>
            {selectedQuickShortcuts.length === 0 ? (
              <div className="flex flex-col gap-3 rounded-lg border border-dashed p-4 text-sm text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
                <span>{t('home.quickActions.empty')}</span>
                <Button variant="outline" size="sm" onClick={() => setQuickShortcutDialogOpen(true)}>
                  {t('home.quickActions.add')}
                </Button>
              </div>
            ) : (
              <div className="flex flex-wrap gap-2">
                {selectedQuickShortcuts.map((shortcut) => {
                  const Icon = shortcut.icon
                  const content = (
                    <>
                      <Icon className={`h-4 w-4 ${shortcut.id === 'action:restart' && isRestarting ? 'animate-spin' : ''}`} />
                      <span className="min-w-0 flex-1 truncate text-left">
                        {shortcut.label}
                      </span>
                      {shortcut.badge && (
                        <span className="ml-1 shrink-0 rounded-full bg-orange-500 px-1.5 py-0.5 text-xs text-white">
                          {shortcut.badge}
                        </span>
                      )}
                      {shortcut.external && <ExternalLink className="h-3.5 w-3.5 shrink-0" />}
                    </>
                  )

                  if (shortcut.href) {
                    return (
                      <Button
                        key={shortcut.id}
                        variant="outline"
                        size="sm"
                        asChild
                        className="max-w-[14rem] justify-start gap-2 overflow-hidden sm:max-w-[18rem]"
                      >
                        <a
                          href={shortcut.href}
                          target={shortcut.external ? '_blank' : undefined}
                          rel={shortcut.external ? 'noopener noreferrer' : undefined}
                          title={shortcut.label}
                        >
                          {content}
                        </a>
                      </Button>
                    )
                  }

                  return (
                    <Button
                      key={shortcut.id}
                      variant="outline"
                      size="sm"
                      onClick={shortcut.action}
                      disabled={shortcut.disabled}
                      className="max-w-[14rem] justify-start gap-2 overflow-hidden sm:max-w-[18rem]"
                      title={shortcut.label}
                    >
                      {content}
                    </Button>
                  )
                })}
              </div>
            )}
          </CardContent>
        </Card>

      </div>

      <div className="grid gap-4 grid-cols-1 xl:grid-cols-[minmax(0,1fr)_320px]">
        {/* 统计概览 */}
        <Card>
          <CardHeader className="flex flex-col gap-3 pb-3 sm:flex-row sm:items-start sm:justify-between">
            <div className="space-y-1.5">
              <CardTitle className="flex h-5 items-center gap-2 text-sm font-medium leading-5">
                <BarChart3 className="h-4 w-4" />
                {t('home.stats.overviewTitle')}
              </CardTitle>
              <CardDescription>
                {t('home.stats.recentPeriod', {
                  range: timeRange < 48
                    ? timeRange + t('home.stats.hours')
                    : Math.floor(timeRange / 24) + t('home.stats.days'),
                })}
              </CardDescription>
            </div>
            <Tabs value={timeRange.toString()} onValueChange={(v) => setTimeRange(Number(v))}>
              <TabsList className="grid grid-cols-3">
                <TabsTrigger value="24">{t('home.timeRange.24h')}</TabsTrigger>
                <TabsTrigger value="168">{t('home.timeRange.7d')}</TabsTrigger>
                <TabsTrigger value="720">{t('home.timeRange.30d')}</TabsTrigger>
              </TabsList>
            </Tabs>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-2 2xl:grid-cols-4">
              <div className="rounded-lg border p-4">
                <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
                  <span>{t('home.stats.totalRequests')}</span>
                  <Activity className="h-4 w-4" />
                </div>
                <div className="mt-3 text-2xl font-bold">
                  {formatNumber(summary.total_requests).display}
                  {formatNumber(summary.total_requests).needsExact && (
                    <span className="ml-1 text-xs font-normal text-muted-foreground">
                      ({formatNumber(summary.total_requests).exact})
                    </span>
                  )}
                </div>
              </div>

              <div className="rounded-lg border p-4">
                <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
                  <span>{t('home.stats.totalCost')}</span>
                  <DollarSign className="h-4 w-4" />
                </div>
                <div className="mt-3 text-2xl font-bold">
                  {formatCurrency(summary.total_cost).display}
                  {formatCurrency(summary.total_cost).needsExact && (
                    <span className="ml-1 text-xs font-normal text-muted-foreground">
                      ({formatCurrency(summary.total_cost).exact})
                    </span>
                  )}
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  {summary.cost_per_hour > 0
                    ? t('home.stats.perHour', { value: `¥${summary.cost_per_hour.toFixed(2)}` })
                    : t('home.stats.noData')}
                </p>
              </div>

              <div className="rounded-lg border p-4">
                <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
                  <span>{t('home.stats.tokenUsage')}</span>
                  <Database className="h-4 w-4" />
                </div>
                <div className="mt-3 text-2xl font-bold">
                  {formatNumber(summary.total_tokens).display}
                  {formatNumber(summary.total_tokens).needsExact && (
                    <span className="ml-1 text-xs font-normal text-muted-foreground">
                      ({formatNumber(summary.total_tokens).exact})
                    </span>
                  )}
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  {summary.tokens_per_hour > 0
                    ? t('home.stats.perHour', { value: formatNumber(summary.tokens_per_hour).display })
                    : t('home.stats.noData')}
                </p>
              </div>

              <div className="rounded-lg border p-4">
                <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
                  <span>{t('home.stats.avgResponse')}</span>
                  <Zap className="h-4 w-4" />
                </div>
                <div className="mt-3 text-2xl font-bold">{summary.avg_response_time.toFixed(2)}s</div>
                <p className="mt-1 text-xs text-muted-foreground">{t('home.stats.avgResponseDesc')}</p>
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-3">
              <div className="rounded-lg border p-4">
                <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
                  <span>{t('home.stats.onlineTime')}</span>
                  <Clock className="h-4 w-4" />
                </div>
                <div className="mt-3 text-xl font-bold">
                  {formatTime(summary.online_time)}
                  <span className="ml-1 text-xs font-normal text-muted-foreground">
                    ({summary.online_time.toLocaleString()}{t('home.stats.seconds')})
                  </span>
                </div>
              </div>

              <div className="rounded-lg border p-4">
                <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
                  <span>{t('home.stats.messageProcessing')}</span>
                  <MessageSquare className="h-4 w-4" />
                </div>
                <div className="mt-3 text-xl font-bold">
                  {formatNumber(summary.total_messages).display}
                  {formatNumber(summary.total_messages).needsExact && (
                    <span className="ml-1 text-xs font-normal text-muted-foreground">
                      ({formatNumber(summary.total_messages).exact})
                    </span>
                  )}
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  {t('home.stats.replied', { num: formatNumber(summary.total_replies).display })}
                  {formatNumber(summary.total_replies).needsExact && (
                    <span>({formatNumber(summary.total_replies).exact})</span>
                  )}
                </p>
              </div>

              <div className="rounded-lg border p-4">
                <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
                  <span>{t('home.stats.costEfficiency')}</span>
                  <TrendingUp className="h-4 w-4" />
                </div>
                <div className="mt-3 text-xl font-bold">
                  {summary.total_messages > 0
                    ? `¥${((summary.total_cost / summary.total_messages) * 100).toFixed(2)}`
                    : '¥0.00'}
                </div>
                <p className="mt-1 text-xs text-muted-foreground">{t('home.stats.per100Messages')}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="xl:self-stretch">
          <CardHeader className="pb-3">
            <CardTitle className="flex h-5 items-center gap-2 text-sm font-medium leading-5">
              <HardDrive className="h-4 w-4" />
              {t('home.storage.title')}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              <div>
                <div className="text-2xl font-bold">
                  {hasLocalCacheStats
                    ? formatStorageBytes(totalStorageSize)
                    : isLocalCacheStatsLoading
                      ? t('home.storage.reading')
                      : '-'}
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  {hasLocalCacheStats
                    ? t('home.storage.summary', {
                        image: formatStorageBytes(imageCacheSize),
                        emoji: formatStorageBytes(emojiCacheSize),
                        logs: formatStorageBytes(logCacheSize),
                        database: formatStorageBytes(databaseSize),
                      })
                    : isLocalCacheStatsLoading
                      ? t('home.storage.readingDescription')
                      : t('home.storage.unavailable')}
                </p>
              </div>
              {hasLocalCacheStats && (
                <div className="space-y-2.5">
                  {storageDetails.map((item) => {
                    const Icon = item.icon
                    const percent = totalStorageSize > 0 ? (item.size / totalStorageSize) * 100 : 0
                    const visiblePercent = item.size > 0 ? Math.max(percent, 2) : 0

                    return (
                      <div key={item.key} className="space-y-1.5">
                        <div className="flex items-center justify-between gap-3">
                          <div className="flex min-w-0 items-center gap-2">
                            <Icon className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                            <span className="truncate text-sm font-medium">{item.label}</span>
                          </div>
                          <span className="shrink-0 text-sm font-semibold">{formatStorageBytes(item.size)}</span>
                        </div>
                        <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                          <div
                            className="h-full rounded-full bg-primary transition-all"
                            style={{ width: `${visiblePercent}%` }}
                          />
                        </div>
                        <div className="flex items-center justify-between gap-3 text-xs text-muted-foreground">
                          <span className="truncate">{item.detail}</span>
                          <span className="shrink-0">{percent.toFixed(percent >= 10 ? 0 : 1)}%</span>
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
              <Button variant="outline" size="sm" asChild className="w-full justify-start gap-2">
                <Link to="/settings" search={{ tab: 'local-cache' }}>
                  <HardDrive className="h-4 w-4" />
                  {t('home.storage.manage')}
                </Link>
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* 图表区域 */}
      <Tabs defaultValue="trends" className="space-y-4">
        <TabsList className="grid w-full grid-cols-2 sm:grid-cols-4">
          <TabsTrigger value="trends">{t('home.charts.tabs.trends')}</TabsTrigger>
          <TabsTrigger value="models">{t('home.charts.tabs.models')}</TabsTrigger>
          <TabsTrigger value="activity">{t('home.charts.tabs.activity')}</TabsTrigger>
          <TabsTrigger value="daily">{t('home.charts.tabs.daily')}</TabsTrigger>
        </TabsList>

        {/* 趋势图表 */}
        <TabsContent value="trends" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>{t('home.charts.requestTrend')}</CardTitle>
              <CardDescription>{t('home.charts.requestTrendDesc', { hours: timeRange })}</CardDescription>
            </CardHeader>
            <CardContent>
              <ZoomableChart aria-label={t('home.ariaLabel.requestTrend')}>
              <ChartContainer config={chartConfig} className="h-[300px] sm:h-[400px] w-full aspect-auto">
                <LineChart data={hourly_data}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--color-muted-foreground) / 0.2)" />
                  <XAxis
                    dataKey="timestamp"
                    tickFormatter={(value) => formatDateTime(value)}
                    angle={-45}
                    textAnchor="end"
                    height={60}
                    stroke="hsl(var(--color-muted-foreground))"
                    tick={{ fill: 'hsl(var(--color-muted-foreground))' }}
                  />
                  <YAxis stroke="hsl(var(--color-muted-foreground))" tick={{ fill: 'hsl(var(--color-muted-foreground))' }} />
                  <ChartTooltip
                    content={<ChartTooltipContent labelFormatter={(value) => formatDateTime(value as string)} />}
                  />
                  <Line
                    type="monotone"
                    dataKey="requests"
                    stroke="var(--color-requests)"
                    strokeWidth={2}
                  />
                </LineChart>
              </ChartContainer>
              </ZoomableChart>
            </CardContent>
          </Card>

          <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>{t('home.charts.costTrend')}</CardTitle>
                <CardDescription>{t('home.charts.costTrendDesc')}</CardDescription>
              </CardHeader>
              <CardContent>
                <ZoomableChart aria-label={t('home.ariaLabel.costTrend')}>
                <ChartContainer config={chartConfig} className="h-[250px] sm:h-[300px] w-full aspect-auto">
                  <BarChart data={hourly_data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--color-muted-foreground) / 0.2)" />
                    <XAxis
                      dataKey="timestamp"
                      tickFormatter={(value) => formatDateTime(value)}
                      angle={-45}
                      textAnchor="end"
                      height={60}
                      stroke="hsl(var(--color-muted-foreground))"
                      tick={{ fill: 'hsl(var(--color-muted-foreground))' }}
                    />
                    <YAxis stroke="hsl(var(--color-muted-foreground))" tick={{ fill: 'hsl(var(--color-muted-foreground))' }} />
                    <ChartTooltip
                      content={<ChartTooltipContent labelFormatter={(value) => formatDateTime(value as string)} />}
                    />
                    <Bar dataKey="cost" fill="var(--color-cost)" />
                  </BarChart>
                </ChartContainer>
                </ZoomableChart>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t('home.charts.tokenUsage')}</CardTitle>
                <CardDescription>{t('home.charts.tokenUsageDesc')}</CardDescription>
              </CardHeader>
              <CardContent>
                <ZoomableChart aria-label={t('home.ariaLabel.tokenUsage')}>
                <ChartContainer config={chartConfig} className="h-[250px] sm:h-[300px] w-full aspect-auto">
                  <BarChart data={hourly_data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--color-muted-foreground) / 0.2)" />
                    <XAxis
                      dataKey="timestamp"
                      tickFormatter={(value) => formatDateTime(value)}
                      angle={-45}
                      textAnchor="end"
                      height={60}
                      stroke="hsl(var(--color-muted-foreground))"
                      tick={{ fill: 'hsl(var(--color-muted-foreground))' }}
                    />
                    <YAxis stroke="hsl(var(--color-muted-foreground))" tick={{ fill: 'hsl(var(--color-muted-foreground))' }} />
                    <ChartTooltip
                      content={<ChartTooltipContent labelFormatter={(value) => formatDateTime(value as string)} />}
                    />
                    <Bar dataKey="tokens" fill="var(--color-tokens)" />
                  </BarChart>
                </ChartContainer>
                </ZoomableChart>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* 模型统计 */}
        <TabsContent value="models" className="space-y-4">
          <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>{t('home.charts.modelDistribution')}</CardTitle>
                <CardDescription>{t('home.charts.modelDistributionDesc', { count: model_stats.length })}</CardDescription>
              </CardHeader>
              <CardContent>
                <ChartContainer
                  config={
                    Object.fromEntries(
                      model_stats.map((stat, i) => [
                        stat.model_name,
                        {
                          label: stat.model_name,
                          color: pieColors[i],
                        },
                      ])
                    ) as ChartConfig
                  }
                  className="h-[300px] sm:h-[400px] w-full aspect-auto"
                >
                  <PieChart>
                    <ChartTooltip content={<ChartTooltipContent />} />
                    <Pie
                      data={modelPieData}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, percent }) => {
                        // 只显示占比大于5%的标签，避免小块标签重叠
                        if (percent && percent < 0.05) return ''
                        return `${name} ${percent ? (percent * 100).toFixed(0) : 0}%`
                      }}
                      outerRadius={100}
                      dataKey="value"
                      nameKey="name"
                    >
                      {modelPieData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.fill} />
                      ))}
                    </Pie>
                  </PieChart>
                </ChartContainer>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>{t('home.charts.modelDetails')}</CardTitle>
                <CardDescription>{t('home.charts.modelDetailsDesc')}</CardDescription>
              </CardHeader>
              <CardContent>
                <ScrollArea className="h-[300px] sm:h-[400px]">
                  <div className="space-y-3">
                    {model_stats.map((stat, index) => (
                      <div
                        key={index}
                        className="p-4 rounded-lg border bg-card hover:bg-accent/50 transition-colors"
                      >
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-semibold text-sm truncate flex-1 min-w-0">
                            {stat.model_name}
                          </h4>
                          <div
                            className="w-3 h-3 rounded-full ml-2 flex-shrink-0"
                            style={{
                              backgroundColor: pieColors[index],
                            }}
                          />
                        </div>
                        <div className="grid grid-cols-2 gap-2 text-xs">
                          <div>
                            <span className="text-muted-foreground">{t('home.charts.requestCount')}:</span>
                            <span className="ml-1 font-medium">
                              {stat.request_count.toLocaleString()}
                            </span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">{t('home.charts.costLabel')}:</span>
                            <span className="ml-1 font-medium">¥{stat.total_cost.toFixed(2)}</span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">Tokens:</span>
                            <span className="ml-1 font-medium">
                              {(stat.total_tokens / 1000).toFixed(1)}K
                            </span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">{t('home.charts.avgTime')}:</span>
                            <span className="ml-1 font-medium">
                              {stat.avg_response_time.toFixed(2)}s
                            </span>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
        <TabsContent value="activity">
          <Card>
            <CardHeader>
              <CardTitle>{t('home.charts.recentActivity')}</CardTitle>
              <CardDescription>{t('home.charts.recentActivityDesc')}</CardDescription>
            </CardHeader>
            <CardContent>
              <ScrollArea className="h-[400px] sm:h-[500px]">
                <div className="space-y-2">
                  {recent_activity.map((activity, index) => (
                    <div
                      key={index}
                      className="p-3 sm:p-4 rounded-lg border bg-card hover:bg-accent/50 transition-colors"
                    >
                      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-2">
                        <div className="flex-1 min-w-0">
                          <div className="font-medium text-sm truncate">{activity.model}</div>
                          <div className="text-xs text-muted-foreground">
                            {activity.request_type}
                          </div>
                        </div>
                        <div className="text-xs text-muted-foreground flex-shrink-0">
                          {formatDateTime(activity.timestamp)}
                        </div>
                      </div>
                      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs">
                        <div>
                          <span className="text-muted-foreground">Tokens:</span>
                          <span className="ml-1">{activity.tokens}</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">{t('home.charts.costLabel')}:</span>
                          <span className="ml-1">¥{activity.cost.toFixed(4)}</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">{t('home.charts.timeCost')}:</span>
                          <span className="ml-1">{activity.time_cost.toFixed(2)}s</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">{t('home.charts.status')}:</span>
                          <span
                            className={`ml-1 ${activity.status === 'success' ? 'text-green-600' : 'text-red-600'}`}
                          >
                            {activity.status}
                          </span>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </ScrollArea>
            </CardContent>
          </Card>
        </TabsContent>

        {/* 日统计 */}
        <TabsContent value="daily">
          <Card>
            <CardHeader>
              <CardTitle>{t('home.charts.dailyStats')}</CardTitle>
              <CardDescription>{t('home.charts.dailyStatsDesc')}</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer
                config={{
                  requests: {
                    label: t('home.charts.requests'),
                    color: 'hsl(var(--color-chart-1))',
                  },
                  cost: {
                    label: t('home.charts.cost'),
                    color: 'hsl(var(--color-chart-2))',
                  },
                }}
                className="h-[400px] sm:h-[500px] w-full aspect-auto"
              >
                <BarChart data={daily_data}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--color-muted-foreground) / 0.2)" />
                  <XAxis
                    dataKey="timestamp"
                    tickFormatter={(value) => {
                      const date = new Date(value)
                      return new Intl.DateTimeFormat(currentLocale, {
                        month: 'numeric',
                        day: 'numeric',
                      }).format(date)
                    }}
                    stroke="hsl(var(--color-muted-foreground))"
                    tick={{ fill: 'hsl(var(--color-muted-foreground))' }}
                  />
                  <YAxis yAxisId="left" stroke="hsl(var(--color-muted-foreground))" tick={{ fill: 'hsl(var(--color-muted-foreground))' }} />
                  <YAxis yAxisId="right" orientation="right" stroke="hsl(var(--color-muted-foreground))" tick={{ fill: 'hsl(var(--color-muted-foreground))' }} />
                  <ChartTooltip
                    content={
                      <ChartTooltipContent
                        labelFormatter={(value) => {
                          const date = new Date(value as string)
                          return date.toLocaleDateString(currentLocale)
                        }}
                      />
                    }
                  />
                  <ChartLegend content={<ChartLegendContent />} />
                  <Bar yAxisId="left" dataKey="requests" fill="var(--color-requests)" />
                  <Bar yAxisId="right" dataKey="cost" fill="var(--color-cost)" />
                </BarChart>
              </ChartContainer>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      <Dialog open={quickShortcutDialogOpen} onOpenChange={setQuickShortcutDialogOpen}>
        <DialogContent style={{ '--dialog-width': '46rem' } as CSSProperties}>
          <DialogHeader>
            <DialogTitle>{t('home.quickActions.dialog.title')}</DialogTitle>
            <DialogDescription>
              {t('home.quickActions.dialog.description')}
            </DialogDescription>
          </DialogHeader>
          <DialogBody viewportClassName="max-h-[60vh]">
            <div className="space-y-4 pr-1">
              <Input
                value={quickShortcutSearch}
                onChange={(event) => setQuickShortcutSearch(event.target.value)}
                placeholder={t('home.quickActions.dialog.searchPlaceholder')}
              />
              <div className="space-y-2">
                {filteredQuickShortcutOptions.map((shortcut) => {
                  const Icon = shortcut.icon
                  const checked = quickShortcutIds.includes(shortcut.id)
                  const checkboxId = `quick-shortcut-${shortcut.id}`
                  return (
                    <label
                      key={shortcut.id}
                      htmlFor={checkboxId}
                      className="flex cursor-pointer items-start gap-3 rounded-lg border p-3 transition-colors hover:bg-accent/40"
                    >
                      <Checkbox
                        id={checkboxId}
                        className="mt-0.5"
                        checked={checked}
                        onCheckedChange={(value) => toggleQuickShortcut(shortcut.id, value === true)}
                      />
                      <Icon className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
                      <span className="min-w-0 flex-1">
                        <span className="flex flex-wrap items-center gap-2">
                          <span className="font-medium">{shortcut.label}</span>
                          <Badge variant="outline" className="text-[10px]">
                            {t(`home.quickActions.categories.${shortcut.category}`)}
                          </Badge>
                        </span>
                        <span className="mt-1 block text-sm text-muted-foreground">
                          {shortcut.description}
                        </span>
                      </span>
                    </label>
                  )
                })}
                {filteredQuickShortcutOptions.length === 0 && (
                  <div className="rounded-lg border border-dashed p-6 text-center text-sm text-muted-foreground">
                    {isPluginShortcutsLoading
                      ? t('home.quickActions.dialog.loadingPluginEntries')
                      : t('home.quickActions.dialog.noMatches')}
                  </div>
                )}
              </div>
            </div>
          </DialogBody>
          <DialogFooter>
            <Button variant="outline" onClick={resetQuickShortcuts}>
              {t('home.quickActions.dialog.restoreDefault')}
            </Button>
            <Button onClick={() => setQuickShortcutDialogOpen(false)}>
              {t('home.quickActions.dialog.done')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 重启遮罩层 */}
      <RestartOverlay />

      {/* 表达方式审核器 */}
      <ExpressionReviewer
        open={isReviewerOpen}
        onOpenChange={(open) => {
          setIsReviewerOpen(open)
          if (!open) {
            // 关闭审核器时刷新统计
            fetchReviewStats()
          }
        }}
      />
    </div>
    </ScrollArea>
  )
}
