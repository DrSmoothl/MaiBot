import { AlertCircle, CheckCircle2, Download, Info, Loader2, RefreshCw, ThumbsUp, Trash2, TrendingUp } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'

import { PluginIcon } from './PluginIcon'
import type { GitStatus, MaimaiVersion, PluginInfo, PluginLoadProgress, PluginStatsData } from './types'
import { getPluginProgressDetail, getPluginTypeLabel } from './types'

interface PluginCardProps {
  plugin: PluginInfo
  gitStatus: GitStatus | null
  maimaiVersion: MaimaiVersion | null
  pluginStats: Record<string, PluginStatsData>
  loadProgress: PluginLoadProgress | null
  isAnyPluginInstalling?: boolean
  likingPluginIds: Set<string>
  onInstall: (plugin: PluginInfo) => void
  onLike: (plugin: PluginInfo) => void
  onUpdate: (plugin: PluginInfo) => void
  onUninstall: (plugin: PluginInfo) => void
  onDetail: (plugin: PluginInfo) => void
  checkPluginCompatibility: (plugin: PluginInfo) => boolean
  needsUpdate: (plugin: PluginInfo) => boolean
  getStatusBadge: (plugin: PluginInfo) => React.JSX.Element | null
  getIncompatibleReason: (plugin: PluginInfo) => string | null
}

function getRecentPluginActivity(plugin: PluginInfo): { timeLabel: string; action: string } | null {
  const parseTime = (value?: string) => value ? Date.parse(value) : NaN
  const releaseTimes = (plugin.releases?.versions ?? [])
    .filter((release) => !release.prerelease && !release.yanked)
    .map((release) => parseTime(release.published_at))
    .filter(Number.isFinite)
  const publishedTime = parseTime(plugin.published_at)
  const firstPublishedTime = Number.isFinite(publishedTime)
    ? publishedTime
    : releaseTimes.length > 0 ? Math.min(...releaseTimes) : NaN
  const activityTimes = [firstPublishedTime, parseTime(plugin.updated_at), ...releaseTimes]
    .filter(Number.isFinite)
  if (activityTimes.length === 0) return null

  // 首次发布时间之后的市场改动或正式版本发布都算更新，只展示最近七天的活动。
  const latestTime = Math.max(...activityTimes)
  const ageMinutes = (Date.now() - latestTime) / 60_000
  if (ageMinutes < 0 || ageMinutes >= 7 * 24 * 60) return null

  const timeLabel = ageMinutes >= 24 * 60
    ? `${Math.floor(ageMinutes / (24 * 60))} 天前`
    : ageMinutes >= 60
      ? `${Math.floor(ageMinutes / 60)} H前`
      : `${Math.floor(ageMinutes)} 分钟前`
  return {
    timeLabel,
    action: latestTime === firstPublishedTime ? '发布' : '更新',
  }
}

function getPluginGrowthRank(rank7d?: number | null, rank30d?: number | null): { rank: number; days: number } | null {
  // 仅展示前八名；同一指标优先展示短期飙升，避免四套位次挤占卡片标题。
  if (rank7d != null && Number.isInteger(rank7d) && rank7d >= 1 && rank7d <= 8) {
    return { rank: rank7d, days: 7 }
  }
  if (rank30d != null && Number.isInteger(rank30d) && rank30d >= 1 && rank30d <= 8) {
    return { rank: rank30d, days: 30 }
  }
  return null
}

export function PluginCard({
  plugin,
  gitStatus,
  maimaiVersion,
  pluginStats,
  loadProgress,
  isAnyPluginInstalling = false,
  likingPluginIds,
  onInstall,
  onLike,
  onUpdate,
  onUninstall,
  onDetail,
  checkPluginCompatibility,
  needsUpdate,
  getStatusBadge,
  getIncompatibleReason,
}: PluginCardProps) {
  const stats = [plugin.manifest?.id]
    .map(id => id ? pluginStats[id] : undefined)
    .find(Boolean)
  const likeCount = stats?.likes ?? 0
  const downloadCount = stats?.downloads ?? plugin.downloads ?? 0
  const ratingValue = stats?.rating ?? plugin.rating ?? 0
  const reviewCount = stats?.comment_count ?? plugin.review_count ?? 0
  const isLiked = stats?.liked === true
  const isLiking = likingPluginIds.has(plugin.manifest?.id || plugin.id)
  const isInstalling = loadProgress?.operation === 'install'
    && loadProgress.stage === 'loading'
    && loadProgress?.plugin_id === plugin.id
  const isPluginOperating = loadProgress?.stage === 'loading'
    && loadProgress.operation !== 'fetch'
  const progressDetail = loadProgress ? getPluginProgressDetail(loadProgress) : null
  const recentActivity = getRecentPluginActivity(plugin)
  const downloadGrowth = getPluginGrowthRank(stats?.downloads_growth_rank_7d, stats?.downloads_growth_rank_30d)
  const likeGrowth = getPluginGrowthRank(stats?.likes_growth_rank_7d, stats?.likes_growth_rank_30d)
  // 下载与点赞只保留名次更靠前的一项；同名次优先短期榜，再优先下载榜。
  const showDownloadGrowth = downloadGrowth !== null && (
    likeGrowth === null
    || downloadGrowth.rank < likeGrowth.rank
    || (downloadGrowth.rank === likeGrowth.rank && downloadGrowth.days <= likeGrowth.days)
  )

  return (
    <Card
      key={plugin.id}
      data-plugin-market-card="true"
      data-plugin-recent-activity={recentActivity || downloadGrowth || likeGrowth ? 'true' : undefined}
      className="flex h-full flex-col"
    >
      <CardHeader className="p-4 pb-2.5">
        <div className="flex items-start justify-between gap-2">
          <div className="flex min-w-0 flex-1 items-start gap-2.5">
            <div className="flex h-[4.125rem] w-12 shrink-0 flex-col items-center gap-1.5">
              <PluginIcon
                pluginId={plugin.id}
                manifest={plugin.manifest}
                installed={plugin.installed}
                marketplaceIconUrl={plugin.assets?.icon_64}
                className="h-12 w-12 rounded-md"
                iconClassName="h-5 w-5"
              />
              <span
                data-plugin-type-label="true"
                className="text-primary whitespace-nowrap text-center text-xs font-semibold leading-none"
              >
                {getPluginTypeLabel(plugin)}
              </span>
            </div>
            <CardTitle className="h-[4.125rem] min-w-0 flex-1 text-lg leading-[1.2]">
              <span className="line-clamp-2 break-words">
                {plugin.manifest?.name || plugin.id}
              </span>
            </CardTitle>
          </div>
          <div className="flex shrink-0 flex-col items-end gap-2">
            {getStatusBadge(plugin)}
            {showDownloadGrowth && downloadGrowth && (
              <div
                className="flex items-center gap-1 rounded-sm bg-orange-700 px-1.5 py-1 text-[11px] font-semibold text-white shadow-sm dark:bg-orange-400 dark:text-orange-950"
                title={`${downloadGrowth.days} 天下载飙升榜第 ${downloadGrowth.rank} 名，按本期比上期的增加量排序`}
              >
                <TrendingUp className="h-3 w-3 shrink-0" />
                <span className="whitespace-nowrap">{downloadGrowth.days}日下载飙升 #{downloadGrowth.rank}</span>
              </div>
            )}
            {!showDownloadGrowth && likeGrowth && (
              <div
                className="flex items-center gap-1 rounded-sm bg-rose-700 px-1.5 py-1 text-[11px] font-semibold text-white shadow-sm dark:bg-rose-400 dark:text-rose-950"
                title={`${likeGrowth.days} 天点赞飙升榜第 ${likeGrowth.rank} 名，按本期比上期的增加量排序，仅统计当前有效的赞`}
              >
                <TrendingUp className="h-3 w-3 shrink-0" />
                <span className="whitespace-nowrap">{likeGrowth.days}日点赞飙升 #{likeGrowth.rank}</span>
              </div>
            )}
            {recentActivity && (
              <div
                className={`flex flex-col border-r-2 pr-2 text-right text-[11px] leading-tight ${recentActivity.action === '发布' ? 'border-emerald-500/70' : 'border-orange-500/70'}`}
                title={`${recentActivity.timeLabel}${recentActivity.action}`}
              >
                <span className="text-muted-foreground whitespace-nowrap">{recentActivity.timeLabel}</span>
                <span className="mt-0.5 font-medium">{recentActivity.action}</span>
              </div>
            )}
          </div>
        </div>
        <CardDescription className="line-clamp-3 min-h-[3.09375rem] text-xs leading-snug">
          {plugin.manifest?.description || '无描述'}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex-1 px-4 pb-2.5">
        <div className="space-y-2">
          {/* 统计信息 */}
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <div className="flex items-center gap-1">
              <span>下载</span>
              <span
                data-plugin-stat-value="downloads"
                className={downloadCount !== 0 ? 'text-primary' : undefined}
              >
                {downloadCount.toLocaleString()}
              </span>
            </div>
            <div className="flex items-center gap-1">
              <span>评分</span>
              <span
                data-plugin-stat-value="rating"
                className={ratingValue !== 0 ? 'text-primary' : undefined}
              >
                {ratingValue.toFixed(1)}
              </span>
            </div>
            <div className="flex items-center gap-1">
              <span>点赞</span>
              <span
                data-plugin-stat-value="likes"
                className={likeCount !== 0 ? 'text-primary' : undefined}
              >
                {likeCount.toLocaleString()}
              </span>
            </div>
            <div className="flex items-center gap-1">
              <span>评论</span>
              <span
                data-plugin-stat-value="reviews"
                className={reviewCount !== 0 ? 'text-primary' : undefined}
              >
                {reviewCount.toLocaleString()}
              </span>
            </div>
          </div>
          {/* 标签 */}
          <div className="flex flex-wrap gap-1.5">
            {plugin.manifest?.keywords && plugin.manifest.keywords.slice(0, 3).map((keyword) => (
              <Badge key={keyword} variant="outline" className="px-1.5 py-0 text-[11px]">
                {keyword}
              </Badge>
            ))}
            {plugin.manifest?.keywords && plugin.manifest.keywords.length > 3 && (
              <Badge variant="outline" className="px-1.5 py-0 text-[11px]">
                +{plugin.manifest.keywords.length - 3}
              </Badge>
            )}
          </div>
        </div>
      </CardContent>
      <CardFooter className="mt-auto px-4 pb-4 pt-0">
        <div className="flex w-full flex-col gap-2 border-t pt-2.5 sm:flex-row sm:items-end sm:justify-between">
          {/* 版本、作者和支持版本 */}
          <div
            data-plugin-version-info="true"
            className="text-muted-foreground min-w-0 space-y-0.5 text-[11px] leading-tight"
          >
            <div className="truncate">
              v{plugin.manifest?.version || 'unknown'} · {plugin.manifest?.author?.name || 'Unknown'}
            </div>
            {plugin.manifest?.host_application && (
              <div className="flex items-center gap-1 whitespace-nowrap">
                <span>支持:</span>
                <span className="font-medium">
                  {plugin.manifest.host_application.min_version}
                  {plugin.manifest.host_application.max_version
                    ? ` - ${plugin.manifest.host_application.max_version}`
                    : ' - 最新版本'}
                </span>
              </div>
            )}
          </div>
          <div className="grid shrink-0 grid-cols-3 gap-2 sm:flex sm:items-center sm:justify-end">
          <Button
            variant={isLiked ? 'secondary' : 'outline'}
            size="sm"
            className="w-full px-2 sm:w-auto"
            title={isLiked ? '取消点赞' : '点赞'}
            aria-label={isLiked ? '取消点赞' : '点赞'}
            disabled={isLiking}
            onClick={() => onLike(plugin)}
          >
            {isLiking ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <ThumbsUp className={isLiked ? 'h-4 w-4 fill-current' : 'h-4 w-4'} />
            )}
            <span>{likeCount.toLocaleString()}</span>
          </Button>
          <Button 
            variant="outline"
            size="sm"
            className="w-full px-0 sm:w-8"
            title="查看详情"
            aria-label="查看详情"
            onClick={() => onDetail(plugin)}
          >
            <Info className="h-4 w-4" />
          </Button>
          {plugin.installed ? (
            needsUpdate(plugin) ? (
              <Button 
                size="sm"
                className="w-full sm:w-auto"
                disabled={
                  !gitStatus?.installed
                  || isPluginOperating
                  || (maimaiVersion !== null && !checkPluginCompatibility(plugin))
                }
                title={
                  !gitStatus?.installed
                    ? 'Git 未安装'
                    : isPluginOperating
                      ? '插件操作进行中'
                    : (maimaiVersion !== null && !checkPluginCompatibility(plugin))
                      ? (getIncompatibleReason(plugin) ?? '插件与当前麦麦版本不兼容')
                      : undefined
                }
                onClick={() => onUpdate(plugin)}
              >
                {isPluginOperating ? (
                  <Loader2 className="mr-1 h-4 w-4 animate-spin" />
                ) : (
                  <RefreshCw className="mr-1 h-4 w-4" />
                )}
                {isPluginOperating ? '更新中' : '更新'}
              </Button>
            ) : (
              <Button 
                variant="destructive" 
                size="sm"
                className="w-full px-0 sm:w-8"
                disabled={!gitStatus?.installed || isPluginOperating}
                title={
                  !gitStatus?.installed
                    ? 'Git 未安装'
                    : isPluginOperating
                      ? '插件操作进行中'
                      : '卸载'
                }
                aria-label="卸载"
                onClick={() => onUninstall(plugin)}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            )
          ) : (
            <Button 
              size="sm"
              className="w-full px-0 sm:w-8"
              disabled={
                !gitStatus?.installed || 
                isAnyPluginInstalling ||
                (maimaiVersion !== null && !checkPluginCompatibility(plugin))
              }
              title={
                !gitStatus?.installed 
                  ? 'Git 未安装' 
                  : (maimaiVersion !== null && !checkPluginCompatibility(plugin))
                    ? (getIncompatibleReason(plugin) ?? '插件与当前麦麦版本不兼容')
                    : undefined
              }
              aria-label={isInstalling ? '正在安装' : '安装'}
              onClick={() => onInstall(plugin)}
            >
              {isInstalling ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
            </Button>
          )}
          </div>
        </div>
      </CardFooter>
      {/* 安装/卸载/更新进度显示 - 在卡片下方 */}
      {loadProgress && 
        (loadProgress.stage === 'loading' || loadProgress.stage === 'success' || loadProgress.stage === 'error') && 
        loadProgress.operation !== 'fetch' && 
        loadProgress.plugin_id === plugin.id && (
        <div className="-mt-1 px-4 pb-4">
          <div className={`space-y-2 rounded-lg border p-2.5 ${
            loadProgress.stage === 'success' 
              ? 'bg-green-50 dark:bg-green-950/20 border-green-200 dark:border-green-900' 
              : loadProgress.stage === 'error'
                ? 'bg-red-50 dark:bg-red-950/20 border-red-200 dark:border-red-900'
                : 'bg-muted/50'
          }`}>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                {loadProgress.stage === 'loading' ? (
                  <Loader2 className="h-3 w-3 animate-spin" />
                ) : loadProgress.stage === 'success' ? (
                  <CheckCircle2 className="h-3 w-3 text-green-600" />
                ) : (
                  <AlertCircle className="h-3 w-3 text-red-600" />
                )}
                <span className={`text-xs font-medium ${
                  loadProgress.stage === 'success' 
                    ? 'text-green-700 dark:text-green-300' 
                    : loadProgress.stage === 'error'
                      ? 'text-red-700 dark:text-red-300'
                      : ''
                }`}>
                  {loadProgress.stage === 'loading' ? (
                    <>
                      {loadProgress.operation === 'install' && '正在安装'}
                      {loadProgress.operation === 'uninstall' && '正在卸载'}
                      {loadProgress.operation === 'update' && '正在更新'}
                    </>
                  ) : loadProgress.stage === 'success' ? (
                    <>
                      {loadProgress.operation === 'install' && '安装完成'}
                      {loadProgress.operation === 'uninstall' && '卸载完成'}
                      {loadProgress.operation === 'update' && '更新完成'}
                    </>
                  ) : (
                    <>
                      {loadProgress.operation === 'install' && '安装失败'}
                      {loadProgress.operation === 'uninstall' && '卸载失败'}
                      {loadProgress.operation === 'update' && '更新失败'}
                    </>
                  )}
                </span>
              </div>
              {loadProgress.stage !== 'error' && (
                <span className={`text-xs font-medium ${
                  loadProgress.stage === 'success' ? 'text-green-700 dark:text-green-300' : ''
                }`}>{loadProgress.progress}%</span>
              )}
            </div>
            {loadProgress.stage !== 'error' && (
              <Progress 
                value={loadProgress.progress} 
                className={`h-1.5 ${loadProgress.stage === 'success' ? '[&>div]:bg-green-500' : ''}`} 
              />
            )}
            <div className={`text-xs ${
              loadProgress.stage === 'success' 
                ? 'text-green-600 dark:text-green-400 truncate' 
                : loadProgress.stage === 'error'
                  ? 'text-red-600 dark:text-red-400'
                  : 'text-muted-foreground truncate'
            }`}>
              {loadProgress.stage === 'error' ? (loadProgress.error || loadProgress.message || '操作失败') : loadProgress.message}
            </div>
            {progressDetail && (
              <div className="truncate text-xs text-muted-foreground">
                {progressDetail}
              </div>
            )}
          </div>
        </div>
      )}
    </Card>
  )
}
