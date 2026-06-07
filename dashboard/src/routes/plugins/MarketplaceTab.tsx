import { useMemo } from 'react'
import { Sparkles } from 'lucide-react'

import type { GitStatus, MaimaiVersion, MarketplaceSortKey, PluginInfo, PluginLoadProgress, PluginStatsData } from './types'
import { getPluginType } from './types'
import { PluginCard } from './PluginCard'

const SURPRISE_PLUGIN_COUNT = 4
const SURPRISE_CANDIDATE_LIMIT = 20
const FRESHNESS_BOOST_WEIGHT = 4
const FRESHNESS_BOOST_WINDOW_DAYS = 120
const LAUNCH_BOOST_WEIGHT = 12
const LAUNCH_BOOST_FULL_HOURS = 24
const LAUNCH_BOOST_DECAY_HOURS = 48
const MS_PER_DAY = 24 * 60 * 60 * 1000
const MS_PER_HOUR = 60 * 60 * 1000

interface MarketplaceTabProps {
  plugins: PluginInfo[]
  searchQuery: string
  pluginTypeFilter: string
  showCompatibleOnly: boolean
  hideInstalledPlugins: boolean
  sortBy: MarketplaceSortKey
  gitStatus: GitStatus | null
  maimaiVersion: MaimaiVersion | null
  pluginStats: Record<string, PluginStatsData>
  loadProgress: PluginLoadProgress | null
  onInstall: (plugin: PluginInfo) => void
  onUpdate: (plugin: PluginInfo) => void
  onUninstall: (plugin: PluginInfo) => void
  checkPluginCompatibility: (plugin: PluginInfo) => boolean
  needsUpdate: (plugin: PluginInfo) => boolean
  getStatusBadge: (plugin: PluginInfo) => React.JSX.Element | null
  getIncompatibleReason: (plugin: PluginInfo) => string | null
}

function getPluginIdentity(plugin: PluginInfo): string {
  return plugin.manifest?.id || plugin.id || plugin.marketplace_id || plugin.manifest?.name
}

function parsePluginTime(value: string | undefined): number {
  if (!value) {
    return 0
  }

  const time = Date.parse(value)
  return Number.isNaN(time) ? 0 : time
}

function getPluginFreshness(plugin: PluginInfo): number {
  const publishedTime = parsePluginTime(plugin.published_at)
  if (publishedTime > 0) {
    return publishedTime
  }

  const updatedTime = parsePluginTime(plugin.updated_at)
  if (updatedTime > 0) {
    return updatedTime
  }

  return plugin.marketplace_order ?? 0
}

function getFreshnessBoost(plugin: PluginInfo, maxMarketplaceOrder: number, now: number): number {
  const publishedTime = parsePluginTime(plugin.published_at)
  const updatedTime = parsePluginTime(plugin.updated_at)
  const pluginTime = publishedTime > 0 ? publishedTime : updatedTime

  if (pluginTime > 0) {
    const ageDays = Math.max(0, (now - pluginTime) / MS_PER_DAY)
    if (ageDays >= FRESHNESS_BOOST_WINDOW_DAYS) {
      return 0
    }

    return (1 - ageDays / FRESHNESS_BOOST_WINDOW_DAYS) * FRESHNESS_BOOST_WEIGHT
  }

  if (maxMarketplaceOrder <= 0) {
    return 0
  }

  return ((plugin.marketplace_order ?? 0) / maxMarketplaceOrder) * FRESHNESS_BOOST_WEIGHT
}

function getLaunchBoost(plugin: PluginInfo, now: number): number {
  const publishedTime = parsePluginTime(plugin.published_at)
  const updatedTime = parsePluginTime(plugin.updated_at)
  const pluginTime = publishedTime > 0 ? publishedTime : updatedTime

  if (pluginTime <= 0) {
    return 0
  }

  const ageHours = Math.max(0, (now - pluginTime) / MS_PER_HOUR)
  if (ageHours <= LAUNCH_BOOST_FULL_HOURS) {
    return LAUNCH_BOOST_WEIGHT
  }
  if (ageHours >= LAUNCH_BOOST_DECAY_HOURS) {
    return 0
  }

  const decayProgress = (ageHours - LAUNCH_BOOST_FULL_HOURS) / (LAUNCH_BOOST_DECAY_HOURS - LAUNCH_BOOST_FULL_HOURS)
  return (1 - decayProgress) * LAUNCH_BOOST_WEIGHT
}

function getStableRandomRank(seed: string, plugin: PluginInfo): number {
  const value = `${seed}:${getPluginIdentity(plugin)}`
  let hash = 2166136261

  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }

  return hash >>> 0
}

function selectSurprisePlugins(
  plugins: PluginInfo[],
  sortBy: MarketplaceSortKey,
  seed: string
): PluginInfo[] {
  if (sortBy !== 'default' || plugins.length <= SURPRISE_PLUGIN_COUNT) {
    return []
  }

  const candidateCount = Math.min(
    SURPRISE_CANDIDATE_LIMIT,
    Math.max(SURPRISE_PLUGIN_COUNT, Math.ceil(plugins.length * 0.3))
  )

  return [...plugins]
    .sort((left, right) => {
      const freshnessDiff = getPluginFreshness(right) - getPluginFreshness(left)
      if (freshnessDiff !== 0) {
        return freshnessDiff
      }

      return (right.marketplace_order ?? 0) - (left.marketplace_order ?? 0)
    })
    .slice(0, candidateCount)
    .sort((left, right) => getStableRandomRank(seed, left) - getStableRandomRank(seed, right))
    .slice(0, SURPRISE_PLUGIN_COUNT)
}

export function MarketplaceTab({
  plugins,
  searchQuery,
  pluginTypeFilter,
  showCompatibleOnly,
  hideInstalledPlugins,
  sortBy,
  gitStatus,
  maimaiVersion,
  pluginStats,
  loadProgress,
  onInstall,
  onUpdate,
  onUninstall,
  checkPluginCompatibility,
  needsUpdate,
  getStatusBadge,
  getIncompatibleReason,
}: MarketplaceTabProps) {
  const surpriseSeed = useMemo(() => Math.random().toString(36).slice(2), [])

  // 过滤插件
  const getPluginStats = (plugin: PluginInfo): PluginStatsData | undefined => {
    const statsIds = [
      plugin.manifest?.id,
      plugin.id,
    ].filter((id): id is string => Boolean(id))

    return statsIds.map((id) => pluginStats[id]).find(Boolean)
  }

  const getSortValue = (plugin: PluginInfo, maxMarketplaceOrder: number, now: number): number => {
    const stats = getPluginStats(plugin)

    if (sortBy === 'default') {
      const downloads = stats?.downloads ?? plugin.downloads ?? 0
      const likes = stats?.likes ?? 0
      const rating = stats?.rating ?? plugin.rating ?? 0
      const ratingCount = stats?.rating_count ?? 0

      return Math.log10(downloads + 1) * 4
        + Math.log10(likes + 1) * 3
        + rating * Math.log10(ratingCount + 2) * 2
        + getLaunchBoost(plugin, now)
        + getFreshnessBoost(plugin, maxMarketplaceOrder, now)
    }
    if (sortBy === 'latest') {
      return getPluginFreshness(plugin)
    }
    if (sortBy === 'downloads') {
      return stats?.downloads ?? plugin.downloads ?? 0
    }
    if (sortBy === 'likes') {
      return stats?.likes ?? 0
    }
    if (sortBy === 'rating') {
      return stats?.rating ?? plugin.rating ?? 0
    }

    return 0
  }

  const matchedPlugins = plugins.filter(plugin => {
    // 跳过没有 manifest 的插件
    if (!plugin.manifest) {
      console.warn('[过滤] 跳过无 manifest 的插件:', plugin.id)
      return false
    }

    // 全部插件只展示 plugin-repo 中存在的市场插件，本地独有插件只在“已安装”显示。
    if (plugin.source === 'local') {
      return false
    }

    if (hideInstalledPlugins && plugin.installed) {
      return false
    }
    
    // 搜索过滤
    const matchesSearch = searchQuery === '' ||
      plugin.manifest.name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      plugin.manifest.description?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (plugin.manifest.keywords && plugin.manifest.keywords.some(k => k.toLowerCase().includes(searchQuery.toLowerCase())))
    
    // 类型过滤
    const matchesType = pluginTypeFilter === 'all' || getPluginType(plugin) === pluginTypeFilter
    
    // 兼容性过滤
    const matchesCompatibility = !showCompatibleOnly || 
      !maimaiVersion || 
      checkPluginCompatibility(plugin)
    
    return matchesSearch && matchesType && matchesCompatibility
  })
  const maxMarketplaceOrder = matchedPlugins.reduce(
    (maxOrder, plugin) => Math.max(maxOrder, plugin.marketplace_order ?? 0),
    0
  )
  const now = Date.now()
  const filteredPlugins = matchedPlugins.sort((left, right) => {
    const valueDiff = getSortValue(right, maxMarketplaceOrder, now) - getSortValue(left, maxMarketplaceOrder, now)
    if (valueDiff !== 0) {
      return valueDiff
    }

    const freshnessDiff = getPluginFreshness(right) - getPluginFreshness(left)
    if (freshnessDiff !== 0) {
      return freshnessDiff
    }

    return (left.manifest?.name || left.id).localeCompare(right.manifest?.name || right.id)
  })

  const surprisePlugins = selectSurprisePlugins(filteredPlugins, sortBy, surpriseSeed)
  const surprisePluginIds = new Set(surprisePlugins.map(getPluginIdentity))
  const mainPlugins = filteredPlugins.filter(plugin => !surprisePluginIds.has(getPluginIdentity(plugin)))

  const renderPluginCard = (plugin: PluginInfo) => (
    <PluginCard
      key={plugin.id}
      plugin={plugin}
      gitStatus={gitStatus}
      maimaiVersion={maimaiVersion}
      pluginStats={pluginStats}
      loadProgress={loadProgress}
      onInstall={onInstall}
      onUpdate={onUpdate}
      onUninstall={onUninstall}
      checkPluginCompatibility={checkPluginCompatibility}
      needsUpdate={needsUpdate}
      getStatusBadge={getStatusBadge}
      getIncompatibleReason={getIncompatibleReason}
    />
  )

  return (
    <div className="space-y-6">
      {surprisePlugins.length > 0 && (
        <section className="space-y-3">
          <div className="flex items-center gap-2">
            <Sparkles className="h-4 w-4 text-primary" />
            <h2 className="text-base font-semibold">惊喜随意</h2>
          </div>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
            {surprisePlugins.map(renderPluginCard)}
          </div>
        </section>
      )}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
        {mainPlugins.map(renderPluginCard)}
      </div>
    </div>
  )
}
