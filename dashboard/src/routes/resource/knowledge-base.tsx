import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import {
  Database,
  Loader2,
  RefreshCw,
  RotateCcw,
  SlidersHorizontal,
  Upload,
  CheckCircle2,
  CircleAlert,
  FolderOpen,
  HardDrive,
  X,
} from 'lucide-react'

import { MemoryDeleteDialog } from '@/components/memory/MemoryDeleteDialog'
import { MemoryEpisodeManager } from '@/components/memory/MemoryEpisodeManager'
import { MemoryMaintenanceManager } from '@/components/memory/MemoryMaintenanceManager'
import { MemoryMiniTabs } from '@/components/memory/MemoryMiniTabs'
import { MemoryProfileManager } from '@/components/memory/MemoryProfileManager'
import { MemoryTimelineManager } from '@/components/memory/MemoryTimelineManager'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Tabs, TabsContent } from '@/components/ui/tabs'
import { Textarea } from '@/components/ui/textarea'
import { ThinkingIllustration } from '@/components/ui/thinking-illustration'
import { useToast } from '@/hooks/use-toast'
import { memoryProgressClient, type MemoryProgressEvent } from '@/lib/memory-progress-client'
import { cn } from '@/lib/utils'
import {
  cancelMemoryImportTask,
  createMemoryLpmmConvertImport,
  createMemoryLpmmOpenieImport,
  createMemoryMaibotMigrationImport,
  createMemoryRawScanImport,
  createMemoryTemporalBackfillImport,
  executeMemoryDelete,
  getMemoryFeedbackCorrection,
  getMemoryFeedbackCorrections,
  getMemoryImportPathAliases,
  getMemoryImportChatTargets,
  getMemoryImportSettings,
  getMemoryImportTask,
  getMemoryImportTaskChunks,
  applyBestMemoryTuningProfile,
  createMemoryPasteImport,
  createMemoryTuningTask,
  createMemoryUploadImport,
  getMemoryDeleteOperation,
  getMemoryDeleteOperations,
  getMemoryImportTasks,
  getMemoryRuntimeConfig,
  getMemorySources,
  getMemoryTuningProfile,
  getMemoryTuningTasks,
  rebuildMemoryRuntimeVectors,
  type MemoryDeleteRequestPayload,
  type MemoryImportChunkListPayload,
  type MemoryImportChatTargetPayload,
  type MemoryImportInputMode,
  type MemoryImportSettings,
  type MemoryImportTaskKind,
  type MemoryImportTaskPayload,
  previewMemoryDelete,
  refreshMemoryRuntimeSelfCheck,
  rollbackMemoryFeedbackCorrection,
  resolveMemoryImportPath,
  retryMemoryImportTask,
  restoreMemoryDelete,
  type MemoryDeleteExecutePayload,
  type MemoryDeleteOperationPayload,
  type MemoryFeedbackActionLogPayload,
  type MemoryFeedbackCorrectionDetailTaskPayload,
  type MemoryFeedbackCorrectionSummaryPayload,
  type MemorySourceItemPayload,
  type MemoryRuntimeConfigPayload,
  type MemoryTaskPayload,
  type MemoryTimelineJumpTargetPayload,
} from '@/lib/memory-api'

import {
  DELETE_OPERATION_FETCH_LIMIT,
  DELETE_OPERATION_ITEM_PAGE_SIZE,
  DELETE_OPERATION_PAGE_SIZE,
  FEEDBACK_ACTION_LOG_PAGE_SIZE,
  FEEDBACK_CORRECTION_FETCH_LIMIT,
  FEEDBACK_CORRECTION_PAGE_SIZE,
  IMPORT_CHUNK_PAGE_SIZE,
  QUEUED_IMPORT_STATUS,
  RUNNING_IMPORT_STATUS,
} from './knowledge-base/constants'
import {
  buildFeedbackImpactSummary,
  getFeedbackCorrectionPreview,
  parseCommaSeparatedList,
  parseOptionalNonNegativeInt,
  parseOptionalPositiveInt,
  summarizeFeedbackActionPayload,
} from './knowledge-base/utils'
import { DeleteTab } from './knowledge-base/tabs/DeleteTab'
import { FeedbackTab } from './knowledge-base/tabs/FeedbackTab'
import { ImportTab } from './knowledge-base/tabs/ImportTab'
import { TuningTab } from './knowledge-base/tabs/TuningTab'
import { KnowledgeGraphPage } from './knowledge-graph'

const DATE_TIME_LOCAL_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?$/
const MEMORY_QUICK_START_DISMISSED_KEY = 'memory-quick-start-dismissed'
const POSITIVE_INTEGER_PATTERN = /^[1-9]\d*$/
type MemoryConsoleTab = 'graph' | 'timeline' | 'import' | 'tuning' | 'episodes' | 'profiles' | 'maintenance' | 'delete' | 'feedback'
type LoadableMemoryTab = Extract<MemoryConsoleTab, 'timeline' | 'import' | 'tuning' | 'delete' | 'feedback'>

const MEMORY_CONSOLE_TABS: MemoryConsoleTab[] = [
  'graph',
  'timeline',
  'import',
  'tuning',
  'episodes',
  'profiles',
  'maintenance',
  'delete',
  'feedback',
]

interface KnowledgeBaseDeepLinkState {
  tab: MemoryConsoleTab
  chatId?: string
  timeStart?: number
  timeEnd?: number
  episodeId?: string
  source?: string
  personId?: string
  taskId?: number
  operationId?: string
  maintenanceTarget?: string
}

function parseMaibotPositiveInt(input: string, fieldName: string): number | undefined {
  const value = input.trim()
  if (!value) {
    return undefined
  }
  if (!POSITIVE_INTEGER_PATTERN.test(value)) {
    throw new Error(`${fieldName} 必须填写正整数`)
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${fieldName} 超过可支持的整数范围`)
  }
  return parsed
}

function getMaibotDateTimeLocalTimestamp(input: string, fieldName: string): number | undefined {
  const value = input.trim()
  if (!value) {
    return undefined
  }
  if (!DATE_TIME_LOCAL_PATTERN.test(value)) {
    throw new Error(`${fieldName}格式无效，请使用时间选择器填写`)
  }
  const timestamp = new Date(value).getTime()
  if (!Number.isFinite(timestamp)) {
    throw new Error(`${fieldName}不是有效时间`)
  }
  return timestamp
}

function formatMaibotDateTimeLocalForApi(input: string, fieldName: string): string | undefined {
  const value = input.trim()
  if (!value) {
    return undefined
  }
  if (!DATE_TIME_LOCAL_PATTERN.test(value)) {
    throw new Error(`${fieldName}格式无效，请使用时间选择器填写`)
  }
  const date = new Date(value)
  const timestamp = date.getTime()
  if (!Number.isFinite(timestamp)) {
    throw new Error(`${fieldName}不是有效时间`)
  }
  return date.toISOString()
}

function parseOptionalTimestampQuery(value: string | null): number | undefined {
  if (!value) {
    return undefined
  }
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : undefined
}

function readKnowledgeBaseDeepLink(): KnowledgeBaseDeepLinkState {
  if (typeof window === 'undefined') {
    return { tab: 'graph' }
  }
  const params = new URLSearchParams(window.location.search)
  const tabParam = params.get('tab') as MemoryConsoleTab | null
  const tab = tabParam && MEMORY_CONSOLE_TABS.includes(tabParam) ? tabParam : 'graph'
  const taskId = parseOptionalTimestampQuery(params.get('task_id'))
  return {
    tab,
    chatId: params.get('chat_id') || undefined,
    timeStart: parseOptionalTimestampQuery(params.get('from') ?? params.get('time_start')),
    timeEnd: parseOptionalTimestampQuery(params.get('to') ?? params.get('time_end')),
    episodeId: params.get('episode_id') || undefined,
    source: params.get('source') || undefined,
    personId: params.get('person_id') || undefined,
    taskId: taskId ? Math.floor(taskId) : undefined,
    operationId: params.get('operation_id') || undefined,
    maintenanceTarget: params.get('target') || undefined,
  }
}

function updateKnowledgeBaseDeepLink(tab: MemoryConsoleTab, updates: Record<string, string | number | undefined>) {
  if (typeof window === 'undefined') {
    return
  }
  const params = new URLSearchParams()
  params.set('tab', tab)
  Object.entries(updates).forEach(([key, value]) => {
    if (value !== undefined && String(value).trim()) {
      params.set(key, String(value))
    }
  })
  const nextUrl = `${window.location.pathname}?${params.toString()}${window.location.hash}`
  window.history.replaceState(null, '', nextUrl)
}

function readJumpParam(target: MemoryTimelineJumpTargetPayload, key: string): string {
  const value = target.params?.[key]
  if (value === undefined || value === null) {
    return ''
  }
  return String(value)
}

function readJumpNumber(target: MemoryTimelineJumpTargetPayload, key: string): number | undefined {
  const value = Number(readJumpParam(target, key))
  return Number.isFinite(value) ? value : undefined
}

export function KnowledgeBasePage() {
  const { toast } = useToast()
  const deepLinkRef = useRef<KnowledgeBaseDeepLinkState>(readKnowledgeBaseDeepLink())
  const [loading, setLoading] = useState(true)
  const [refreshingCheck, setRefreshingCheck] = useState(false)
  const [vectorRebuildDialogOpen, setVectorRebuildDialogOpen] = useState(false)
  const [vectorRebuilding, setVectorRebuilding] = useState(false)
  const [vectorRebuildPreview, setVectorRebuildPreview] = useState<Record<string, number> | null>(null)
  const [creatingImport, setCreatingImport] = useState(false)
  const [creatingTuning, setCreatingTuning] = useState(false)
  const [activeTab, setActiveTab] = useState<MemoryConsoleTab>(deepLinkRef.current.tab)
  const [quickStartVisible, setQuickStartVisible] = useState(() => {
    if (typeof window === 'undefined') {
      return true
    }
    return window.localStorage.getItem(MEMORY_QUICK_START_DISMISSED_KEY) !== 'true'
  })
  const [visitedMemoryTabs, setVisitedMemoryTabs] = useState<Set<MemoryConsoleTab>>(() => new Set(['graph', deepLinkRef.current.tab]))
  const [tabLoading, setTabLoading] = useState<Partial<Record<LoadableMemoryTab, boolean>>>({})
  const loadedPanelDataRef = useRef<Set<LoadableMemoryTab>>(new Set())
  const [timelineInitialChatId] = useState(deepLinkRef.current.chatId ?? '')
  const [timelineInitialTimeStart] = useState<number | undefined>(deepLinkRef.current.timeStart)
  const [timelineInitialTimeEnd] = useState<number | undefined>(deepLinkRef.current.timeEnd)
  const [episodeInitialTarget, setEpisodeInitialTarget] = useState({
    episodeId: deepLinkRef.current.episodeId ?? '',
    source: deepLinkRef.current.source ?? '',
    timeStart: deepLinkRef.current.timeStart,
    timeEnd: deepLinkRef.current.timeEnd,
  })
  const [profileInitialPersonId, setProfileInitialPersonId] = useState(deepLinkRef.current.personId ?? '')
  const [maintenanceInitialTarget, setMaintenanceInitialTarget] = useState(deepLinkRef.current.maintenanceTarget ?? '')

  const [runtimeConfig, setRuntimeConfig] = useState<MemoryRuntimeConfigPayload | null>(null)
  const [importSettings, setImportSettings] = useState<MemoryImportSettings>({})
  const [importPathAliases, setImportPathAliases] = useState<Record<string, string>>({})
  const [importChatTargets, setImportChatTargets] = useState<MemoryImportChatTargetPayload[]>([])
  const [importTasks, setImportTasks] = useState<MemoryImportTaskPayload[]>([])
  const [selectedImportTaskId, setSelectedImportTaskId] = useState('')
  const [selectedImportTask, setSelectedImportTask] = useState<MemoryImportTaskPayload | null>(null)
  const [selectedImportTaskLoading, setSelectedImportTaskLoading] = useState(false)
  const [selectedImportFileId, setSelectedImportFileId] = useState('')
  const [importChunkOffset, setImportChunkOffset] = useState(0)
  const [importChunksPayload, setImportChunksPayload] = useState<MemoryImportChunkListPayload | null>(null)
  const [importChunksLoading, setImportChunksLoading] = useState(false)
  const [importCreateMode, setImportCreateMode] = useState<MemoryImportTaskKind>('upload')
  const [importAutoPolling, setImportAutoPolling] = useState(true)
  const [importErrorText, setImportErrorText] = useState('')
  const [importCommonFileConcurrency, setImportCommonFileConcurrency] = useState('2')
  const [importCommonChunkConcurrency, setImportCommonChunkConcurrency] = useState('4')
  const [importCommonNarrativeWindowSize, setImportCommonNarrativeWindowSize] = useState('1600')
  const [importCommonNarrativeOverlap, setImportCommonNarrativeOverlap] = useState('400')
  const [importCommonFactualTargetSize, setImportCommonFactualTargetSize] = useState('1200')
  const [importCommonLlmEnabled, setImportCommonLlmEnabled] = useState(true)
  const [importCommonStrategyOverride, setImportCommonStrategyOverride] = useState('auto')
  const [importCommonDedupePolicy, setImportCommonDedupePolicy] = useState('content_hash')
  const [importCommonChatLog, setImportCommonChatLog] = useState(false)
  const [importCommonChatId, setImportCommonChatId] = useState('')
  const [importCommonChatReferenceTime, setImportCommonChatReferenceTime] = useState('')
  const [importCommonForce, setImportCommonForce] = useState(false)
  const [importCommonClearManifest, setImportCommonClearManifest] = useState(false)

  const [uploadInputMode, setUploadInputMode] = useState<MemoryImportInputMode>('text')
  const [uploadFiles, setUploadFiles] = useState<File[]>([])

  const [pasteName, setPasteName] = useState('')
  const [pasteMode, setPasteMode] = useState<MemoryImportInputMode>('text')
  const [pasteContent, setPasteContent] = useState('')

  const [rawAlias, setRawAlias] = useState('raw')
  const [rawRelativePath, setRawRelativePath] = useState('')
  const [rawGlob, setRawGlob] = useState('*')
  const [rawInputMode, setRawInputMode] = useState<MemoryImportInputMode>('text')
  const [rawRecursive, setRawRecursive] = useState(true)

  const [openieAlias, setOpenieAlias] = useState('lpmm')
  const [openieRelativePath, setOpenieRelativePath] = useState('')
  const [openieIncludeAllJson, setOpenieIncludeAllJson] = useState(false)

  const [convertAlias, setConvertAlias] = useState('lpmm')
  const [convertRelativePath, setConvertRelativePath] = useState('')
  const [convertTargetAlias, setConvertTargetAlias] = useState('plugin_data')
  const [convertTargetRelativePath, setConvertTargetRelativePath] = useState('')
  const [convertDimension, setConvertDimension] = useState('')
  const [convertBatchSize, setConvertBatchSize] = useState('1024')

  const [backfillAlias, setBackfillAlias] = useState('plugin_data')
  const [backfillRelativePath, setBackfillRelativePath] = useState('')
  const [backfillLimit, setBackfillLimit] = useState('100000')
  const [backfillDryRun, setBackfillDryRun] = useState(false)
  const [backfillNoCreatedFallback, setBackfillNoCreatedFallback] = useState(false)

  const [maibotSourceDb, setMaibotSourceDb] = useState('')
  const [maibotTimeFrom, setMaibotTimeFrom] = useState('')
  const [maibotTimeTo, setMaibotTimeTo] = useState('')
  const [maibotStartId, setMaibotStartId] = useState('')
  const [maibotEndId, setMaibotEndId] = useState('')
  const [maibotStreamIds, setMaibotStreamIds] = useState('')
  const [maibotGroupIds, setMaibotGroupIds] = useState('')
  const [maibotUserIds, setMaibotUserIds] = useState('')
  const [maibotReadBatchSize, setMaibotReadBatchSize] = useState('2000')
  const [maibotCommitWindowRows, setMaibotCommitWindowRows] = useState('20000')
  const [maibotEmbedWorkers, setMaibotEmbedWorkers] = useState('')
  const [maibotNoResume, setMaibotNoResume] = useState(false)
  const [maibotResetState, setMaibotResetState] = useState(false)
  const [maibotDryRun, setMaibotDryRun] = useState(false)
  const [maibotVerifyOnly, setMaibotVerifyOnly] = useState(false)
  const maibotSourceDbDefaultAppliedRef = useRef(false)

  const [pathResolveAlias, setPathResolveAlias] = useState('raw')
  const [pathResolveRelativePath, setPathResolveRelativePath] = useState('')
  const [pathResolveMustExist, setPathResolveMustExist] = useState(true)
  const [pathResolveOutput, setPathResolveOutput] = useState('')
  const [resolvingPath, setResolvingPath] = useState(false)

  const [tuningTasks, setTuningTasks] = useState<MemoryTaskPayload[]>([])
  const [tuningProfile, setTuningProfile] = useState<Record<string, unknown>>({})
  const [tuningProfileToml, setTuningProfileToml] = useState('')
  const [memorySources, setMemorySources] = useState<MemorySourceItemPayload[]>([])
  const [deleteOperations, setDeleteOperations] = useState<MemoryDeleteOperationPayload[]>([])
  const [selectedOperationDetail, setSelectedOperationDetail] = useState<MemoryDeleteOperationPayload | null>(null)
  const [selectedOperationDetailLoading, setSelectedOperationDetailLoading] = useState(false)
  const [selectedOperationDetailError, setSelectedOperationDetailError] = useState('')
  const [sourceSearch, setSourceSearch] = useState(deepLinkRef.current.source ?? '')
  const [operationSearch, setOperationSearch] = useState(deepLinkRef.current.operationId ?? '')
  const [operationModeFilter, setOperationModeFilter] = useState('all')
  const [operationStatusFilter, setOperationStatusFilter] = useState('all')
  const [operationPage, setOperationPage] = useState(1)
  const [selectedOperationId, setSelectedOperationId] = useState(deepLinkRef.current.operationId ?? '')
  const [selectedOperationItemSearch, setSelectedOperationItemSearch] = useState('')
  const [selectedOperationItemPage, setSelectedOperationItemPage] = useState(1)
  const [selectedSources, setSelectedSources] = useState<string[]>([])
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [deleteDialogTitle, setDeleteDialogTitle] = useState('删除预览')
  const [deleteDialogDescription, setDeleteDialogDescription] = useState('')
  const [deletePreview, setDeletePreview] = useState<Awaited<ReturnType<typeof previewMemoryDelete>> | null>(null)
  const [deletePreviewError, setDeletePreviewError] = useState<string | null>(null)
  const [deletePreviewLoading, setDeletePreviewLoading] = useState(false)
  const [deleteExecuting, setDeleteExecuting] = useState(false)
  const [deleteRestoring, setDeleteRestoring] = useState(false)
  const [deleteResult, setDeleteResult] = useState<MemoryDeleteExecutePayload | null>(null)
  const [pendingDeleteRequest, setPendingDeleteRequest] = useState<MemoryDeleteRequestPayload | null>(null)
  const [feedbackCorrections, setFeedbackCorrections] = useState<MemoryFeedbackCorrectionSummaryPayload[]>([])
  const [feedbackSearch, setFeedbackSearch] = useState(deepLinkRef.current.taskId ? String(deepLinkRef.current.taskId) : '')
  const [feedbackStatusFilter, setFeedbackStatusFilter] = useState('all')
  const [feedbackRollbackFilter, setFeedbackRollbackFilter] = useState('all')
  const [feedbackPage, setFeedbackPage] = useState(1)
  const [selectedFeedbackTaskId, setSelectedFeedbackTaskId] = useState(deepLinkRef.current.taskId ?? 0)
  const [selectedFeedbackTaskDetail, setSelectedFeedbackTaskDetail] = useState<MemoryFeedbackCorrectionDetailTaskPayload | null>(null)
  const [selectedFeedbackTaskLoading, setSelectedFeedbackTaskLoading] = useState(false)
  const [selectedFeedbackTaskError, setSelectedFeedbackTaskError] = useState('')
  const [feedbackActionLogSearch, setFeedbackActionLogSearch] = useState('')
  const [feedbackActionLogPage, setFeedbackActionLogPage] = useState(1)
  const [feedbackRollbackDialogOpen, setFeedbackRollbackDialogOpen] = useState(false)
  const [feedbackRollbackReason, setFeedbackRollbackReason] = useState('')
  const [feedbackRollingBack, setFeedbackRollingBack] = useState(false)
  const [tuningObjective, setTuningObjective] = useState('precision_priority')
  const [tuningIntensity, setTuningIntensity] = useState('standard')
  const [tuningSampleSize, setTuningSampleSize] = useState('24')
  const [tuningTopKEval, setTuningTopKEval] = useState('20')

  const setPanelLoading = useCallback((tab: LoadableMemoryTab, value: boolean) => {
    setTabLoading((current) => ({ ...current, [tab]: value }))
  }, [])

  const loadRuntimeConfig = useCallback(async () => {
    const runtimePayload = await getMemoryRuntimeConfig()
    setRuntimeConfig(runtimePayload)
  }, [])

  const loadChatTargets = useCallback(async () => {
    const chatTargetsResult = await getMemoryImportChatTargets()
    setImportChatTargets(chatTargetsResult.data ?? [])
    return chatTargetsResult.data ?? []
  }, [])

  const loadImportPanel = useCallback(async (force = false) => {
    if (!force && loadedPanelDataRef.current.has('import')) {
      return
    }
    try {
      setPanelLoading('import', true)
      const [importSettingsPayload, pathAliasPayload, importTaskPayload] = await Promise.all([
        getMemoryImportSettings(),
        getMemoryImportPathAliases(),
        getMemoryImportTasks(20),
        loadChatTargets().catch((error) => {
          console.warn('加载聊天流列表失败，导入面板将继续显示其他数据', error)
          return null
        }),
      ])

      setImportSettings(importSettingsPayload.settings ?? {})
      setImportPathAliases(pathAliasPayload.path_aliases ?? {})
      setImportTasks(importTaskPayload.items ?? [])
      setSelectedImportTaskId((currentTaskId) => {
        if (currentTaskId || (importTaskPayload.items ?? []).length === 0) {
          return currentTaskId
        }
        const initialTaskId = String(importTaskPayload.items?.[0]?.task_id ?? '')
        return initialTaskId || currentTaskId
      })
      setPathResolveAlias((currentAlias) => {
        if (currentAlias) {
          return currentAlias
        }
        const aliasKeys = Object.keys(pathAliasPayload.path_aliases ?? {})
        return aliasKeys[0] ?? currentAlias
      })
      loadedPanelDataRef.current.add('import')
    } catch (error) {
      toast({
        title: '加载导入数据失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setPanelLoading('import', false)
    }
  }, [loadChatTargets, setPanelLoading, toast])

  const loadTimelinePanel = useCallback(async (force = false) => {
    if (!force && loadedPanelDataRef.current.has('timeline')) {
      return
    }
    try {
      setPanelLoading('timeline', true)
      await loadChatTargets()
      loadedPanelDataRef.current.add('timeline')
    } catch (error) {
      toast({
        title: '加载审计聊天流失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setPanelLoading('timeline', false)
    }
  }, [loadChatTargets, setPanelLoading, toast])

  const loadTuningPanel = useCallback(async (force = false) => {
    if (!force && loadedPanelDataRef.current.has('tuning')) {
      return
    }
    try {
      setPanelLoading('tuning', true)
      const [tuningProfilePayload, tuningTaskPayload] = await Promise.all([
        getMemoryTuningProfile(),
        getMemoryTuningTasks(20),
      ])
      setTuningProfile(tuningProfilePayload.profile ?? {})
      setTuningProfileToml(tuningProfilePayload.toml ?? '')
      setTuningTasks(tuningTaskPayload.items ?? [])
      loadedPanelDataRef.current.add('tuning')
    } catch (error) {
      toast({
        title: '加载调优数据失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setPanelLoading('tuning', false)
    }
  }, [setPanelLoading, toast])

  const loadDeletePanel = useCallback(async (force = false) => {
    if (!force && loadedPanelDataRef.current.has('delete')) {
      return
    }
    try {
      setPanelLoading('delete', true)
      const [sourcePayload, deleteOperationPayload] = await Promise.all([
        getMemorySources(),
        getMemoryDeleteOperations(DELETE_OPERATION_FETCH_LIMIT),
      ])
      setMemorySources(sourcePayload.items ?? [])
      setDeleteOperations(deleteOperationPayload.items ?? [])
      loadedPanelDataRef.current.add('delete')
    } catch (error) {
      toast({
        title: '加载删除数据失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setPanelLoading('delete', false)
    }
  }, [setPanelLoading, toast])

  const loadFeedbackPanel = useCallback(async (force = false) => {
    if (!force && loadedPanelDataRef.current.has('feedback')) {
      return
    }
    try {
      setPanelLoading('feedback', true)
      const feedbackCorrectionPayload = await getMemoryFeedbackCorrections({
        limit: FEEDBACK_CORRECTION_FETCH_LIMIT,
      })
      setFeedbackCorrections(feedbackCorrectionPayload.items ?? [])
      loadedPanelDataRef.current.add('feedback')
    } catch (error) {
      toast({
        title: '加载纠错历史失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setPanelLoading('feedback', false)
    }
  }, [setPanelLoading, toast])

  const loadActiveTabData = useCallback(async (tab: MemoryConsoleTab, force = false) => {
    switch (tab) {
      case 'timeline':
        await loadTimelinePanel(force)
        break
      case 'import':
        await loadImportPanel(force)
        break
      case 'tuning':
        await loadTuningPanel(force)
        break
      case 'delete':
        await loadDeletePanel(force)
        break
      case 'feedback':
        await loadFeedbackPanel(force)
        break
      default:
        break
    }
  }, [loadDeletePanel, loadFeedbackPanel, loadImportPanel, loadTimelinePanel, loadTuningPanel])

  const switchMemoryTab = useCallback((tab: MemoryConsoleTab, query: Record<string, string | number | undefined> = {}) => {
    setActiveTab(tab)
    updateKnowledgeBaseDeepLink(tab, query)
  }, [])

  const handleTimelineJump = useCallback((target: MemoryTimelineJumpTargetPayload) => {
    const tab = target.tab as MemoryConsoleTab
    if (!MEMORY_CONSOLE_TABS.includes(tab)) {
      return
    }

    if (tab === 'episodes') {
      const episodeId = readJumpParam(target, 'episode_id')
      const source = readJumpParam(target, 'source')
      setEpisodeInitialTarget({
        episodeId,
        source,
        timeStart: readJumpNumber(target, 'time_start'),
        timeEnd: readJumpNumber(target, 'time_end'),
      })
      switchMemoryTab('episodes', { episode_id: episodeId, source })
      return
    }

    if (tab === 'profiles') {
      const personId = readJumpParam(target, 'person_id')
      setProfileInitialPersonId(personId)
      switchMemoryTab('profiles', { person_id: personId })
      return
    }

    if (tab === 'feedback') {
      const taskId = Math.floor(readJumpNumber(target, 'task_id') ?? 0)
      if (taskId > 0) {
        setSelectedFeedbackTaskId(taskId)
        setFeedbackSearch(String(taskId))
        setFeedbackActionLogPage(1)
      }
      switchMemoryTab('feedback', { task_id: taskId > 0 ? taskId : undefined })
      void loadFeedbackPanel(true)
      return
    }

    if (tab === 'delete') {
      const operationId = readJumpParam(target, 'operation_id')
      const source = readJumpParam(target, 'source')
      const paragraphHash = readJumpParam(target, 'paragraph_hash')
      if (operationId) {
        setSelectedOperationId(operationId)
        setOperationSearch(operationId)
        switchMemoryTab('delete', { operation_id: operationId })
      } else {
        setSourceSearch(source || paragraphHash)
        setOperationSearch(source || paragraphHash)
        switchMemoryTab('delete', { source: source || undefined })
      }
      void loadDeletePanel(true)
      return
    }

    if (tab === 'maintenance') {
      const targetText = readJumpParam(target, 'target')
      setMaintenanceInitialTarget(targetText)
      switchMemoryTab('maintenance', { target: targetText })
      return
    }

    switchMemoryTab(tab)
  }, [loadDeletePanel, loadFeedbackPanel, switchMemoryTab])

  const loadPage = useCallback(async () => {
    try {
      setLoading(true)
      await loadRuntimeConfig()
      await loadActiveTabData(activeTab, true)
    } catch (error) {
      toast({
        title: '加载长期记忆控制台失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setLoading(false)
    }
  }, [activeTab, loadActiveTabData, loadRuntimeConfig, toast])

  useEffect(() => {
    let cancelled = false
    const loadInitialRuntime = async () => {
      try {
        setLoading(true)
        await loadRuntimeConfig()
      } catch (error) {
        if (!cancelled) {
          toast({
            title: '加载长期记忆运行状态失败',
            description: error instanceof Error ? error.message : '未知错误',
            variant: 'destructive',
          })
        }
      } finally {
        if (!cancelled) {
          setLoading(false)
        }
      }
    }
    void loadInitialRuntime()
    return () => {
      cancelled = true
    }
  }, [loadRuntimeConfig, toast])

  useEffect(() => {
    setVisitedMemoryTabs((current) => {
      if (current.has(activeTab)) {
        return current
      }
      const next = new Set(current)
      next.add(activeTab)
      return next
    })
    void loadActiveTabData(activeTab)
  }, [activeTab, loadActiveTabData])

  const runtimeBadges = useMemo(() => {
    if (!runtimeConfig) {
      return []
    }
    return [
      {
        label: '运行状态',
        value: runtimeConfig.runtime_ready ? '就绪' : '未就绪',
        description: runtimeConfig.embedding_degraded ? 'Embedding 降级运行' : '运行时检查通过',
        icon: runtimeConfig.runtime_ready ? CheckCircle2 : CircleAlert,
        className: runtimeConfig.runtime_ready ? 'border-emerald-500/20 bg-emerald-500/5' : 'border-amber-500/20 bg-amber-500/5',
        iconClassName: runtimeConfig.runtime_ready ? 'text-emerald-500' : 'text-amber-500',
      },
      {
        label: 'Embedding 维度',
        value: String(runtimeConfig.embedding_dimension),
        description: runtimeConfig.relation_vectors_enabled ? '关系向量已启用' : '关系向量未启用',
        icon: HardDrive,
        className: 'border-sky-500/20 bg-sky-500/5',
        iconClassName: 'text-sky-500',
      },
      {
        label: '数据目录',
        value: runtimeConfig.data_dir,
        description: '长期记忆存储位置',
        icon: FolderOpen,
        className: 'border-violet-500/20 bg-violet-500/5',
        iconClassName: 'text-violet-500',
      },
    ]
  }, [runtimeConfig])

  const importPollInterval = useMemo(
    () => Math.max(200, Number(importSettings.poll_interval_ms ?? 1000)),
    [importSettings.poll_interval_ms],
  )

  const importAliasKeys = useMemo(
    () => Object.keys(importPathAliases).sort((left, right) => left.localeCompare(right)),
    [importPathAliases],
  )

  const runningImportTasks = useMemo(
    () => importTasks.filter((task) => RUNNING_IMPORT_STATUS.has(String(task.status ?? '').trim())),
    [importTasks],
  )
  const queuedImportTasks = useMemo(
    () => importTasks.filter((task) => QUEUED_IMPORT_STATUS.has(String(task.status ?? '').trim())),
    [importTasks],
  )
  const recentImportTasks = useMemo(
    () =>
      importTasks.filter((task) => {
        const status = String(task.status ?? '').trim()
        return !RUNNING_IMPORT_STATUS.has(status) && !QUEUED_IMPORT_STATUS.has(status)
      }),
    [importTasks],
  )
  const selectedImportTaskSummary = useMemo(() => {
    if (!selectedImportTaskId) {
      return null
    }
    return importTasks.find((task) => task.task_id === selectedImportTaskId) ?? null
  }, [importTasks, selectedImportTaskId])

  const selectedImportFiles = useMemo(() => {
    return Array.isArray(selectedImportTask?.files) ? selectedImportTask.files : []
  }, [selectedImportTask?.files])

  const selectedImportChunks = useMemo(() => {
    return Array.isArray(importChunksPayload?.items) ? importChunksPayload.items : []
  }, [importChunksPayload?.items])

  const selectedImportTaskResolved = selectedImportTask ?? selectedImportTaskSummary
  const selectedImportTaskErrorText = String(selectedImportTaskResolved?.error ?? '').trim()
  const selectedImportRetrySummary = selectedImportTaskResolved?.retry_summary

  const importChunkTotal = Number(importChunksPayload?.total ?? 0)
  const canImportChunkPrev = importChunkOffset > 0
  const canImportChunkNext = importChunkOffset + IMPORT_CHUNK_PAGE_SIZE < importChunkTotal

  const buildCommonImportPayload = useCallback((): Record<string, unknown> => {
    const chatId = importCommonChatId.trim()
    const payload: Record<string, unknown> = {
      llm_enabled: importCommonLlmEnabled,
      strategy_override: importCommonStrategyOverride,
      dedupe_policy: importCommonDedupePolicy,
      chat_log: importCommonChatLog,
      force: importCommonForce,
      clear_manifest: importCommonClearManifest,
    }

    const fileConcurrency = parseOptionalPositiveInt(importCommonFileConcurrency)
    const chunkConcurrency = parseOptionalPositiveInt(importCommonChunkConcurrency)
    const narrativeWindowSize = parseOptionalPositiveInt(importCommonNarrativeWindowSize)
    const narrativeOverlap = parseOptionalNonNegativeInt(importCommonNarrativeOverlap)
    const factualTargetSize = parseOptionalPositiveInt(importCommonFactualTargetSize)
    if (fileConcurrency !== undefined) {
      payload.file_concurrency = fileConcurrency
    }
    if (chunkConcurrency !== undefined) {
      payload.chunk_concurrency = chunkConcurrency
    }
    if (narrativeWindowSize !== undefined) {
      payload.narrative_window_size = narrativeWindowSize
    }
    if (narrativeOverlap !== undefined) {
      payload.narrative_overlap = narrativeOverlap
    }
    if (factualTargetSize !== undefined) {
      payload.factual_target_size = factualTargetSize
    }
    if (importCommonChatReferenceTime.trim()) {
      payload.chat_reference_time = importCommonChatReferenceTime.trim()
    }
    if (chatId) {
      payload.chat_id = chatId
    }
    return payload
  }, [
    importCommonChatId,
    importCommonChatLog,
    importCommonChatReferenceTime,
    importCommonChunkConcurrency,
    importCommonClearManifest,
    importCommonDedupePolicy,
    importCommonFactualTargetSize,
    importCommonFileConcurrency,
    importCommonForce,
    importCommonLlmEnabled,
    importCommonNarrativeOverlap,
    importCommonNarrativeWindowSize,
    importCommonStrategyOverride,
  ])

  const refreshImportQueue = useCallback(async (silent: boolean = false) => {
    try {
      const [taskPayload, settingsPayload, pathAliasPayload, chatTargetsResult] = await Promise.all([
        getMemoryImportTasks(20),
        getMemoryImportSettings(),
        getMemoryImportPathAliases(),
        getMemoryImportChatTargets().catch((error) => {
          console.warn('刷新聊天流列表失败，保留当前聊天流选项', error)
          return null
        }),
      ])
      const nextTasks = taskPayload.items ?? []
      setImportTasks(nextTasks)
      setImportSettings(settingsPayload.settings ?? {})
      setImportPathAliases(pathAliasPayload.path_aliases ?? {})
      if (chatTargetsResult) {
        setImportChatTargets(chatTargetsResult.data ?? [])
      }
      setImportErrorText('')
      loadedPanelDataRef.current.add('import')

      if (nextTasks.length <= 0) {
        setSelectedImportTaskId('')
        setSelectedImportTask(null)
        setSelectedImportFileId('')
        setImportChunksPayload(null)
        return
      }

      if (!selectedImportTaskId || !nextTasks.some((item) => item.task_id === selectedImportTaskId)) {
        setSelectedImportTaskId(nextTasks[0].task_id)
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : '刷新导入任务失败'
      setImportErrorText(message)
      if (!silent) {
        toast({
          title: '刷新导入任务失败',
          description: message,
          variant: 'destructive',
        })
      }
    }
  }, [selectedImportTaskId, toast])

  const loadImportChunks = useCallback(
    async (
      taskId: string,
      fileId: string,
      offset: number = 0,
      silent: boolean = false,
    ) => {
      if (!taskId || !fileId) {
        setImportChunksPayload(null)
        return
      }
      try {
        setImportChunksLoading(true)
        const payload = await getMemoryImportTaskChunks(taskId, fileId, offset, IMPORT_CHUNK_PAGE_SIZE)
        if (!payload.success) {
          throw new Error(payload.error || '加载分块详情失败')
        }
        setImportChunksPayload(payload)
        setImportErrorText('')
      } catch (error) {
        const message = error instanceof Error ? error.message : '加载分块详情失败'
        setImportChunksPayload(null)
        setImportErrorText(message)
        if (!silent) {
          toast({
            title: '加载分块详情失败',
            description: message,
            variant: 'destructive',
          })
        }
      } finally {
        setImportChunksLoading(false)
      }
    },
    [toast],
  )

  const loadImportTaskDetail = useCallback(
    async (taskId: string, silent: boolean = false) => {
      if (!taskId) {
        setSelectedImportTask(null)
        setSelectedImportFileId('')
        setImportChunksPayload(null)
        return
      }
      try {
        if (!silent) {
          setSelectedImportTaskLoading(true)
        }
        const payload = await getMemoryImportTask(taskId, false)
        if (!payload.success || !payload.task) {
          throw new Error(payload.error || '任务不存在')
        }
        const task = payload.task
        setSelectedImportTask(task)
        setImportErrorText('')
        const files = Array.isArray(task.files) ? task.files : []
        const keepCurrentFile = files.some((file) => file.file_id === selectedImportFileId)
        const nextFileId = keepCurrentFile ? selectedImportFileId : String(files[0]?.file_id ?? '')
        const nextOffset = keepCurrentFile ? importChunkOffset : 0
        if (!keepCurrentFile) {
          setImportChunkOffset(0)
        }
        setSelectedImportFileId(nextFileId)
        if (nextFileId) {
          await loadImportChunks(taskId, nextFileId, nextOffset, silent)
        } else {
          setImportChunksPayload(null)
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : '加载导入任务详情失败'
        setSelectedImportTask(null)
        setSelectedImportFileId('')
        setImportChunksPayload(null)
        setImportErrorText(message)
        if (!silent) {
          toast({
            title: '加载导入任务详情失败',
            description: message,
            variant: 'destructive',
          })
        }
      } finally {
        if (!silent) {
          setSelectedImportTaskLoading(false)
        }
      }
    },
    [importChunkOffset, loadImportChunks, selectedImportFileId, toast],
  )

  const afterImportTaskCreated = useCallback(
    async (taskId: string, successTitle: string) => {
      await refreshImportQueue(true)
      if (taskId) {
        setSelectedImportTaskId(taskId)
        await loadImportTaskDetail(taskId, true)
      }
      toast({
        title: successTitle,
        description: taskId ? `任务 ${taskId.slice(0, 12)} 已加入导入队列` : '导入任务已加入队列',
      })
    },
    [loadImportTaskDetail, refreshImportQueue, toast],
  )

  const submitUploadImport = useCallback(async () => {
    if (uploadFiles.length <= 0) {
      toast({
        title: '请选择上传文件',
        description: '至少选择一个 txt/md/json 文件后再提交',
        variant: 'destructive',
      })
      return
    }
    try {
      setCreatingImport(true)
      const payload = {
        ...buildCommonImportPayload(),
        input_mode: uploadInputMode,
      }
      const result = await createMemoryUploadImport(uploadFiles, payload)
      if (!result.success) {
        throw new Error(result.error || '创建上传导入任务失败')
      }
      const taskId = String(result.task?.task_id ?? '')
      setUploadFiles([])
      await afterImportTaskCreated(taskId, '上传导入任务已创建')
    } catch (error) {
      const message = error instanceof Error ? error.message : '创建上传导入任务失败'
      setImportErrorText(message)
      toast({
        title: '创建上传导入任务失败',
        description: message,
        variant: 'destructive',
      })
    } finally {
      setCreatingImport(false)
    }
  }, [afterImportTaskCreated, buildCommonImportPayload, toast, uploadFiles, uploadInputMode])

  const submitPasteImport = useCallback(async () => {
    if (!pasteContent.trim()) {
      toast({
        title: '粘贴内容不能为空',
        description: '请填写导入内容后再提交',
        variant: 'destructive',
      })
      return
    }
    try {
      setCreatingImport(true)
      const result = await createMemoryPasteImport({
        ...buildCommonImportPayload(),
        name: pasteName || undefined,
        content: pasteContent,
        input_mode: pasteMode,
      })
      if (!result.success) {
        throw new Error(result.error || '创建粘贴导入任务失败')
      }
      const taskId = String(result.task?.task_id ?? '')
      setPasteContent('')
      setPasteName('')
      await afterImportTaskCreated(taskId, '粘贴导入任务已创建')
    } catch (error) {
      const message = error instanceof Error ? error.message : '创建粘贴导入任务失败'
      setImportErrorText(message)
      toast({
        title: '创建粘贴导入任务失败',
        description: message,
        variant: 'destructive',
      })
    } finally {
      setCreatingImport(false)
    }
  }, [afterImportTaskCreated, buildCommonImportPayload, pasteContent, pasteMode, pasteName, toast])

  const submitRawScanImport = useCallback(async () => {
    try {
      setCreatingImport(true)
      const result = await createMemoryRawScanImport({
        ...buildCommonImportPayload(),
        alias: rawAlias,
        relative_path: rawRelativePath,
        glob: rawGlob,
        recursive: rawRecursive,
        input_mode: rawInputMode,
      })
      if (!result.success) {
        throw new Error(result.error || '创建本地扫描任务失败')
      }
      await afterImportTaskCreated(String(result.task?.task_id ?? ''), '本地扫描任务已创建')
    } catch (error) {
      const message = error instanceof Error ? error.message : '创建本地扫描任务失败'
      setImportErrorText(message)
      toast({
        title: '创建本地扫描任务失败',
        description: message,
        variant: 'destructive',
      })
    } finally {
      setCreatingImport(false)
    }
  }, [
    afterImportTaskCreated,
    buildCommonImportPayload,
    rawAlias,
    rawGlob,
    rawInputMode,
    rawRecursive,
    rawRelativePath,
    toast,
  ])

  const submitOpenieImport = useCallback(async () => {
    try {
      setCreatingImport(true)
      const result = await createMemoryLpmmOpenieImport({
        ...buildCommonImportPayload(),
        alias: openieAlias,
        relative_path: openieRelativePath,
        include_all_json: openieIncludeAllJson,
      })
      if (!result.success) {
        throw new Error(result.error || '创建 LPMM OpenIE 任务失败')
      }
      await afterImportTaskCreated(String(result.task?.task_id ?? ''), 'LPMM OpenIE 任务已创建')
    } catch (error) {
      const message = error instanceof Error ? error.message : '创建 LPMM OpenIE 任务失败'
      setImportErrorText(message)
      toast({
        title: '创建 LPMM OpenIE 任务失败',
        description: message,
        variant: 'destructive',
      })
    } finally {
      setCreatingImport(false)
    }
  }, [
    afterImportTaskCreated,
    buildCommonImportPayload,
    openieAlias,
    openieIncludeAllJson,
    openieRelativePath,
    toast,
  ])

  const submitConvertImport = useCallback(async () => {
    try {
      setCreatingImport(true)
      const result = await createMemoryLpmmConvertImport({
        alias: convertAlias,
        relative_path: convertRelativePath,
        target_alias: convertTargetAlias,
        target_relative_path: convertTargetRelativePath,
        dimension: parseOptionalPositiveInt(convertDimension),
        batch_size: parseOptionalPositiveInt(convertBatchSize),
      })
      if (!result.success) {
        throw new Error(result.error || '创建 LPMM 转换任务失败')
      }
      await afterImportTaskCreated(String(result.task?.task_id ?? ''), 'LPMM 转换任务已创建')
    } catch (error) {
      const message = error instanceof Error ? error.message : '创建 LPMM 转换任务失败'
      setImportErrorText(message)
      toast({
        title: '创建 LPMM 转换任务失败',
        description: message,
        variant: 'destructive',
      })
    } finally {
      setCreatingImport(false)
    }
  }, [
    afterImportTaskCreated,
    convertAlias,
    convertBatchSize,
    convertDimension,
    convertRelativePath,
    convertTargetAlias,
    convertTargetRelativePath,
    toast,
  ])

  const submitBackfillImport = useCallback(async () => {
    try {
      setCreatingImport(true)
      const result = await createMemoryTemporalBackfillImport({
        alias: backfillAlias,
        relative_path: backfillRelativePath,
        limit: parseOptionalPositiveInt(backfillLimit),
        dry_run: backfillDryRun,
        no_created_fallback: backfillNoCreatedFallback,
      })
      if (!result.success) {
        throw new Error(result.error || '创建时序回填任务失败')
      }
      await afterImportTaskCreated(String(result.task?.task_id ?? ''), '时序回填任务已创建')
    } catch (error) {
      const message = error instanceof Error ? error.message : '创建时序回填任务失败'
      setImportErrorText(message)
      toast({
        title: '创建时序回填任务失败',
        description: message,
        variant: 'destructive',
      })
    } finally {
      setCreatingImport(false)
    }
  }, [
    afterImportTaskCreated,
    backfillAlias,
    backfillDryRun,
    backfillLimit,
    backfillNoCreatedFallback,
    backfillRelativePath,
    toast,
  ])

  const submitMaibotMigrationImport = useCallback(async () => {
    try {
      setCreatingImport(true)
      const sourceDb = maibotSourceDb.trim()
      if (!sourceDb) {
        throw new Error('请填写源数据库路径')
      }
      const timeFromTimestamp = getMaibotDateTimeLocalTimestamp(maibotTimeFrom, '起始时间')
      const timeToTimestamp = getMaibotDateTimeLocalTimestamp(maibotTimeTo, '结束时间')
      if (
        timeFromTimestamp !== undefined &&
        timeToTimestamp !== undefined &&
        timeFromTimestamp > timeToTimestamp
      ) {
        throw new Error('起始时间不能晚于结束时间')
      }
      const startId = parseMaibotPositiveInt(maibotStartId, '起始 ID')
      const endId = parseMaibotPositiveInt(maibotEndId, '结束 ID')
      if (startId !== undefined && endId !== undefined && startId > endId) {
        throw new Error('起始 ID 不能大于结束 ID')
      }
      const result = await createMemoryMaibotMigrationImport({
        source_db: sourceDb,
        time_from: formatMaibotDateTimeLocalForApi(maibotTimeFrom, '起始时间'),
        time_to: formatMaibotDateTimeLocalForApi(maibotTimeTo, '结束时间'),
        start_id: startId,
        end_id: endId,
        stream_ids: parseCommaSeparatedList(maibotStreamIds),
        group_ids: parseCommaSeparatedList(maibotGroupIds),
        user_ids: parseCommaSeparatedList(maibotUserIds),
        read_batch_size: parseMaibotPositiveInt(maibotReadBatchSize, '读取批大小'),
        commit_window_rows: parseMaibotPositiveInt(maibotCommitWindowRows, '提交窗口行数'),
        embed_workers: parseMaibotPositiveInt(maibotEmbedWorkers, '向量线程数'),
        no_resume: maibotNoResume,
        reset_state: maibotResetState,
        dry_run: maibotDryRun,
        verify_only: maibotVerifyOnly,
      })
      if (!result.success) {
        throw new Error(result.error || '创建 MaiBot 迁移任务失败')
      }
      await afterImportTaskCreated(String(result.task?.task_id ?? ''), 'MaiBot 迁移任务已创建')
    } catch (error) {
      const message = error instanceof Error ? error.message : '创建 MaiBot 迁移任务失败'
      setImportErrorText(message)
      toast({
        title: '创建 MaiBot 迁移任务失败',
        description: message,
        variant: 'destructive',
      })
    } finally {
      setCreatingImport(false)
    }
  }, [
    afterImportTaskCreated,
    maibotCommitWindowRows,
    maibotDryRun,
    maibotEmbedWorkers,
    maibotEndId,
    maibotGroupIds,
    maibotNoResume,
    maibotReadBatchSize,
    maibotResetState,
    maibotSourceDb,
    maibotStartId,
    maibotStreamIds,
    maibotTimeFrom,
    maibotTimeTo,
    maibotUserIds,
    maibotVerifyOnly,
    toast,
  ])

  const cancelSelectedImportTask = useCallback(async () => {
    if (!selectedImportTaskId) {
      return
    }
    try {
      const payload = await cancelMemoryImportTask(selectedImportTaskId)
      if (!payload.success) {
        throw new Error(payload.error || '取消导入任务失败')
      }
      await refreshImportQueue(true)
      await loadImportTaskDetail(selectedImportTaskId, true)
      toast({
        title: '已请求取消任务',
        description: `任务 ${selectedImportTaskId.slice(0, 12)} 正在取消`,
      })
    } catch (error) {
      const message = error instanceof Error ? error.message : '取消导入任务失败'
      setImportErrorText(message)
      toast({
        title: '取消导入任务失败',
        description: message,
        variant: 'destructive',
      })
    }
  }, [loadImportTaskDetail, refreshImportQueue, selectedImportTaskId, toast])

  const retrySelectedImportTask = useCallback(async () => {
    if (!selectedImportTaskId) {
      return
    }
    try {
      const payload = await retryMemoryImportTask(selectedImportTaskId, {
        overrides: buildCommonImportPayload(),
      })
      if (!payload.success) {
        throw new Error(payload.error || '重试失败项失败')
      }
      const nextTaskId = String(payload.task?.task_id ?? '')
      await refreshImportQueue(true)
      if (nextTaskId) {
        setSelectedImportTaskId(nextTaskId)
        await loadImportTaskDetail(nextTaskId, true)
      } else {
        await loadImportTaskDetail(selectedImportTaskId, true)
      }
      toast({
        title: '重试任务已创建',
        description: nextTaskId ? `重试任务 ${nextTaskId.slice(0, 12)} 已进入队列` : '失败项已提交重试',
      })
    } catch (error) {
      const message = error instanceof Error ? error.message : '重试失败项失败'
      setImportErrorText(message)
      toast({
        title: '重试失败项失败',
        description: message,
        variant: 'destructive',
      })
    }
  }, [buildCommonImportPayload, loadImportTaskDetail, refreshImportQueue, selectedImportTaskId, toast])

  const resolveImportPath = useCallback(async () => {
    if (!pathResolveAlias.trim()) {
      return
    }
    try {
      setResolvingPath(true)
      const payload = await resolveMemoryImportPath({
        alias: pathResolveAlias,
        relative_path: pathResolveRelativePath,
        must_exist: pathResolveMustExist,
      })
      const lines = [
        `路径别名: ${payload.alias}`,
        `相对路径: ${payload.relative_path || '(空)'}`,
        `解析结果: ${payload.resolved_path}`,
        `是否存在: ${String(payload.exists)}`,
        `是否文件: ${String(payload.is_file)}`,
        `是否目录: ${String(payload.is_dir)}`,
      ]
      setPathResolveOutput(lines.join('\n'))
    } catch (error) {
      const message = error instanceof Error ? error.message : '路径解析失败'
      setPathResolveOutput(`解析失败：${message}`)
    } finally {
      setResolvingPath(false)
    }
  }, [pathResolveAlias, pathResolveMustExist, pathResolveRelativePath])

  const selectImportTask = useCallback(
    async (taskId: string) => {
      setSelectedImportTaskId(taskId)
      setImportChunkOffset(0)
      await loadImportTaskDetail(taskId)
    },
    [loadImportTaskDetail],
  )

  const selectImportFile = useCallback(
    async (fileId: string) => {
      if (!selectedImportTaskId) {
        return
      }
      setSelectedImportFileId(fileId)
      setImportChunkOffset(0)
      await loadImportChunks(selectedImportTaskId, fileId, 0)
    },
    [loadImportChunks, selectedImportTaskId],
  )

  const moveImportChunkPage = useCallback(
    async (direction: -1 | 1) => {
      if (!selectedImportTaskId || !selectedImportFileId) {
        return
      }
      const nextOffset =
        direction < 0
          ? Math.max(0, importChunkOffset - IMPORT_CHUNK_PAGE_SIZE)
          : importChunkOffset + IMPORT_CHUNK_PAGE_SIZE
      if (nextOffset === importChunkOffset) {
        return
      }
      setImportChunkOffset(nextOffset)
      await loadImportChunks(selectedImportTaskId, selectedImportFileId, nextOffset)
    },
    [importChunkOffset, loadImportChunks, selectedImportFileId, selectedImportTaskId],
  )

  useEffect(() => {
    if (importAliasKeys.length <= 0) {
      return
    }
    const pickAlias = (current: string, preferred: string): string => {
      if (current && importAliasKeys.includes(current)) {
        return current
      }
      if (importAliasKeys.includes(preferred)) {
        return preferred
      }
      return importAliasKeys[0]
    }
    setRawAlias((current) => pickAlias(current, 'raw'))
    setOpenieAlias((current) => pickAlias(current, 'lpmm'))
    setConvertAlias((current) => pickAlias(current, 'lpmm'))
    setConvertTargetAlias((current) => pickAlias(current, 'plugin_data'))
    setBackfillAlias((current) => pickAlias(current, 'plugin_data'))
    setPathResolveAlias((current) => pickAlias(current, 'raw'))
  }, [importAliasKeys])

  useEffect(() => {
    const defaultFileConcurrency = String(importSettings.default_file_concurrency ?? '').trim()
    const defaultChunkConcurrency = String(importSettings.default_chunk_concurrency ?? '').trim()
    const defaultNarrativeWindowSize = String(importSettings.default_narrative_window_size ?? '').trim()
    const defaultNarrativeOverlap = String(importSettings.default_narrative_overlap ?? '').trim()
    const defaultFactualTargetSize = String(importSettings.default_factual_target_size ?? '').trim()
    if (defaultFileConcurrency && importCommonFileConcurrency === '2') {
      setImportCommonFileConcurrency(defaultFileConcurrency)
    }
    if (defaultChunkConcurrency && importCommonChunkConcurrency === '4') {
      setImportCommonChunkConcurrency(defaultChunkConcurrency)
    }
    if (defaultNarrativeWindowSize && importCommonNarrativeWindowSize === '1600') {
      setImportCommonNarrativeWindowSize(defaultNarrativeWindowSize)
    }
    if (defaultNarrativeOverlap && importCommonNarrativeOverlap === '400') {
      setImportCommonNarrativeOverlap(defaultNarrativeOverlap)
    }
    if (defaultFactualTargetSize && importCommonFactualTargetSize === '1200') {
      setImportCommonFactualTargetSize(defaultFactualTargetSize)
    }
  }, [
    importCommonChunkConcurrency,
    importCommonFactualTargetSize,
    importCommonFileConcurrency,
    importCommonNarrativeOverlap,
    importCommonNarrativeWindowSize,
    importSettings.default_chunk_concurrency,
    importSettings.default_factual_target_size,
    importSettings.default_file_concurrency,
    importSettings.default_narrative_overlap,
    importSettings.default_narrative_window_size,
  ])

  useEffect(() => {
    const defaultSourceDb = String(importSettings.maibot_source_db_default ?? '').trim()
    if (!defaultSourceDb || maibotSourceDbDefaultAppliedRef.current) {
      return
    }
    maibotSourceDbDefaultAppliedRef.current = true
    setMaibotSourceDb((currentSourceDb) => currentSourceDb.trim() ? currentSourceDb : defaultSourceDb)
  }, [importSettings.maibot_source_db_default])

  useEffect(() => {
    if (activeTab !== 'import') {
      return
    }
    if (!selectedImportTaskId && importTasks.length > 0) {
      void selectImportTask(importTasks[0].task_id)
    }
  }, [activeTab, importTasks, selectImportTask, selectedImportTaskId])

  useEffect(() => {
    if (activeTab !== 'import') {
      return
    }
    if (!selectedImportTaskId) {
      setSelectedImportTask(null)
      setSelectedImportFileId('')
      setImportChunksPayload(null)
      return
    }
    if (!importTasks.some((task) => task.task_id === selectedImportTaskId) && importTasks.length > 0) {
      void selectImportTask(importTasks[0].task_id)
      return
    }
    void loadImportTaskDetail(selectedImportTaskId, true)
  }, [activeTab, importTasks, loadImportTaskDetail, selectImportTask, selectedImportTaskId])

  useEffect(() => {
    if (activeTab !== 'import' || !importAutoPolling) {
      return
    }
    const timerId = window.setInterval(() => {
      void refreshImportQueue(true)
      if (selectedImportTaskId) {
        void loadImportTaskDetail(selectedImportTaskId, true)
      }
    }, importPollInterval)
    return () => {
      window.clearInterval(timerId)
    }
  }, [activeTab, importAutoPolling, importPollInterval, loadImportTaskDetail, refreshImportQueue, selectedImportTaskId])

  // 统一 WebSocket 推送：作为轮询的实时增强；后端未广播时由轮询兜底
  const selectedImportTaskIdRef = useRef<string>('')
  useEffect(() => {
    selectedImportTaskIdRef.current = selectedImportTaskId
  }, [selectedImportTaskId])

  useEffect(() => {
    if (activeTab !== 'import') {
      return
    }
    let cancelled = false
    let unsubscribe: (() => Promise<void>) | undefined
    const handleEvent = (event: MemoryProgressEvent) => {
      if (event.topic === 'import_progress') {
        void refreshImportQueue(true)
        if (selectedImportTaskIdRef.current) {
          void loadImportTaskDetail(selectedImportTaskIdRef.current, true)
        }
      }
    }
    void memoryProgressClient
      .subscribe(handleEvent, ['import_progress'])
      .then((cleanup) => {
        if (cancelled) {
          void cleanup()
          return
        }
        unsubscribe = cleanup
      })
      .catch((error) => {
        // 订阅失败不影响轮询兜底
        console.warn('订阅长期记忆 WebSocket 失败，已退化到轮询兜底', error)
      })
    return () => {
      cancelled = true
      if (unsubscribe) {
        void unsubscribe()
      }
    }
  }, [activeTab, loadImportTaskDetail, refreshImportQueue])

  const filteredSources = useMemo(() => {
    const keyword = sourceSearch.trim().toLowerCase()
    if (!keyword) {
      return memorySources
    }
    return memorySources.filter((item) => String(item.source ?? '').toLowerCase().includes(keyword))
  }, [memorySources, sourceSearch])

  const filteredDeleteOperations = useMemo(() => {
    const keyword = operationSearch.trim().toLowerCase()
    return deleteOperations.filter((operation) => {
      const mode = String(operation.mode ?? '').trim()
      const status = String(operation.status ?? '').trim()
      const summary = operation.summary ?? {}
      const sources = Array.isArray(summary.sources) ? summary.sources : []

      if (operationModeFilter !== 'all' && mode !== operationModeFilter) {
        return false
      }
      if (operationStatusFilter !== 'all' && status !== operationStatusFilter) {
        return false
      }
      if (!keyword) {
        return true
      }

      return [
        operation.operation_id,
        operation.reason,
        operation.requested_by,
        mode,
        status,
        ...sources.map((item) => String(item)),
      ]
        .map((item) => String(item ?? '').toLowerCase())
        .some((item) => item.includes(keyword))
    })
  }, [deleteOperations, operationModeFilter, operationSearch, operationStatusFilter])

  const deleteOperationPageCount = Math.max(1, Math.ceil(filteredDeleteOperations.length / DELETE_OPERATION_PAGE_SIZE))
  const pagedDeleteOperations = useMemo(() => {
    const start = (operationPage - 1) * DELETE_OPERATION_PAGE_SIZE
    return filteredDeleteOperations.slice(start, start + DELETE_OPERATION_PAGE_SIZE)
  }, [filteredDeleteOperations, operationPage])

  const selectedDeleteOperation = useMemo(
    () => {
      const matchedOperation = filteredDeleteOperations.find((operation) => operation.operation_id === selectedOperationId)
      if (matchedOperation) {
        return matchedOperation
      }
      if (selectedOperationId) {
        return {
          operation_id: selectedOperationId,
          mode: '',
          status: '',
        } satisfies MemoryDeleteOperationPayload
      }
      return pagedDeleteOperations[0] ?? null
    },
    [filteredDeleteOperations, pagedDeleteOperations, selectedOperationId],
  )

  useEffect(() => {
    setOperationPage(1)
  }, [operationSearch, operationModeFilter, operationStatusFilter])

  useEffect(() => {
    if (operationPage > deleteOperationPageCount) {
      setOperationPage(deleteOperationPageCount)
    }
  }, [deleteOperationPageCount, operationPage])

  useEffect(() => {
    if (!selectedDeleteOperation) {
      if (selectedOperationId) {
        setSelectedOperationId('')
      }
      setSelectedOperationDetail(null)
      setSelectedOperationDetailError('')
      return
    }
    if (selectedDeleteOperation.operation_id !== selectedOperationId) {
      setSelectedOperationId(selectedDeleteOperation.operation_id)
    }
  }, [selectedDeleteOperation, selectedOperationId])

  useEffect(() => {
    if (activeTab !== 'delete') {
      return
    }
    const operationId = selectedDeleteOperation?.operation_id
    if (!operationId) {
      setSelectedOperationDetail(null)
      setSelectedOperationDetailError('')
      return
    }

    let cancelled = false
    setSelectedOperationDetailLoading(true)
    setSelectedOperationDetailError('')

    void getMemoryDeleteOperation(operationId)
      .then((payload) => {
        if (cancelled) {
          return
        }
        if (!payload.success || !payload.operation) {
          setSelectedOperationDetail(null)
          setSelectedOperationDetailError(payload.error || '未能加载删除操作详情')
          return
        }
        setSelectedOperationDetail(payload.operation)
      })
      .catch((error) => {
        if (cancelled) {
          return
        }
        setSelectedOperationDetail(null)
        setSelectedOperationDetailError(error instanceof Error ? error.message : '未能加载删除操作详情')
      })
      .finally(() => {
        if (!cancelled) {
          setSelectedOperationDetailLoading(false)
        }
      })

    return () => {
      cancelled = true
    }
  }, [activeTab, selectedDeleteOperation?.operation_id])

  const toggleSourceSelection = useCallback((source: string, checked: boolean) => {
    setSelectedSources((current) => {
      if (checked) {
        return current.includes(source) ? current : [...current, source]
      }
      return current.filter((item) => item !== source)
    })
  }, [])

  const openSourceDeletePreview = useCallback(async () => {
    if (selectedSources.length <= 0) {
      toast({
        title: '请选择来源',
        description: '至少选择一个来源后再进行删除预览',
        variant: 'destructive',
      })
      return
    }
    const request: MemoryDeleteRequestPayload = {
      mode: 'source',
      selector: { sources: selectedSources },
      reason: 'knowledge_base_source_delete',
      requested_by: 'knowledge_base',
    }
    setDeleteDialogTitle('批量删除来源')
    setDeleteDialogDescription('删除来源只会删除该来源下的段落，以及失去全部证据的关系，不会自动删除实体')
    setPendingDeleteRequest(request)
    setDeletePreview(null)
    setDeleteResult(null)
    setDeletePreviewError(null)
    setDeleteDialogOpen(true)
    setDeletePreviewLoading(true)
    try {
      const preview = await previewMemoryDelete(request)
      setDeletePreview(preview)
    } catch (error) {
      setDeletePreviewError(error instanceof Error ? error.message : '删除预览失败')
    } finally {
      setDeletePreviewLoading(false)
    }
  }, [selectedSources, toast])

  const executePendingDelete = useCallback(async () => {
    if (!pendingDeleteRequest) {
      return
    }
    try {
      setDeleteExecuting(true)
      const result = await executeMemoryDelete(pendingDeleteRequest)
      setDeleteResult(result)
      toast({
        title: result.success ? '删除成功' : '删除失败',
        description: result.success ? `操作 ${result.operation_id} 已完成` : result.error || '未能执行删除',
        variant: result.success ? 'default' : 'destructive',
      })
      if (result.success) {
        const [sourcePayload, deleteOperationPayload] = await Promise.all([
          getMemorySources(),
          getMemoryDeleteOperations(DELETE_OPERATION_FETCH_LIMIT),
        ])
        setMemorySources(sourcePayload.items ?? [])
        setDeleteOperations(deleteOperationPayload.items ?? [])
        setSelectedSources([])
      }
    } catch (error) {
      setDeletePreviewError(error instanceof Error ? error.message : '删除失败')
      toast({
        title: '删除失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setDeleteExecuting(false)
    }
  }, [pendingDeleteRequest, toast])

  const restoreDeleteOperation = useCallback(async (operationId: string) => {
    try {
      setDeleteRestoring(true)
      await restoreMemoryDelete({ operation_id: operationId, requested_by: 'knowledge_base' })
      toast({
        title: '恢复成功',
        description: `删除操作 ${operationId} 已恢复`,
      })
      setDeleteDialogOpen(false)
      const [sourcePayload, deleteOperationPayload] = await Promise.all([
        getMemorySources(),
        getMemoryDeleteOperations(DELETE_OPERATION_FETCH_LIMIT),
      ])
      setMemorySources(sourcePayload.items ?? [])
      setDeleteOperations(deleteOperationPayload.items ?? [])
    } catch (error) {
      toast({
        title: '恢复失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setDeleteRestoring(false)
    }
  }, [toast])

  const closeDeleteDialog = useCallback((open: boolean) => {
    if (!open) {
      setDeleteDialogOpen(false)
      setDeletePreview(null)
      setDeleteResult(null)
      setDeletePreviewError(null)
      setPendingDeleteRequest(null)
      return
    }
    setDeleteDialogOpen(true)
  }, [])

  const filteredFeedbackCorrections = useMemo(() => {
    const keyword = feedbackSearch.trim().toLowerCase()
    return feedbackCorrections.filter((item) => {
      const taskStatus = String(item.task_status ?? '').trim().toLowerCase()
      const rollbackStatus = String(item.rollback_status ?? '').trim().toLowerCase()
      if (feedbackStatusFilter !== 'all' && taskStatus !== feedbackStatusFilter) {
        return false
      }
      if (feedbackRollbackFilter !== 'all' && rollbackStatus !== feedbackRollbackFilter) {
        return false
      }
      if (!keyword) {
        return true
      }
      return [
        item.query_tool_id,
        item.session_id,
        item.query_text,
        item.decision,
        item.task_status,
        item.rollback_status,
      ]
        .map((value) => String(value ?? '').toLowerCase())
        .some((value) => value.includes(keyword))
    })
  }, [feedbackCorrections, feedbackRollbackFilter, feedbackSearch, feedbackStatusFilter])

  const feedbackPageCount = Math.max(1, Math.ceil(filteredFeedbackCorrections.length / FEEDBACK_CORRECTION_PAGE_SIZE))
  const pagedFeedbackCorrections = useMemo(() => {
    const start = (feedbackPage - 1) * FEEDBACK_CORRECTION_PAGE_SIZE
    return filteredFeedbackCorrections.slice(start, start + FEEDBACK_CORRECTION_PAGE_SIZE)
  }, [feedbackPage, filteredFeedbackCorrections])

  const selectedFeedbackCorrection = useMemo(
    () => {
      const matchedCorrection = filteredFeedbackCorrections.find((item) => item.task_id === selectedFeedbackTaskId)
      if (matchedCorrection) {
        return matchedCorrection
      }
      if (selectedFeedbackTaskId > 0) {
        return {
          task_id: selectedFeedbackTaskId,
          query_tool_id: '',
          session_id: '',
          query_text: '',
          task_status: '',
          decision: '',
          decision_confidence: 0,
          feedback_message_count: 0,
          rollback_status: '',
          affected_counts: {},
        } satisfies MemoryFeedbackCorrectionSummaryPayload
      }
      return pagedFeedbackCorrections[0] ?? null
    },
    [filteredFeedbackCorrections, pagedFeedbackCorrections, selectedFeedbackTaskId],
  )

  useEffect(() => {
    setFeedbackPage(1)
  }, [feedbackSearch, feedbackStatusFilter, feedbackRollbackFilter])

  useEffect(() => {
    if (feedbackPage > feedbackPageCount) {
      setFeedbackPage(feedbackPageCount)
    }
  }, [feedbackPage, feedbackPageCount])

  useEffect(() => {
    if (!selectedFeedbackCorrection) {
      if (selectedFeedbackTaskId) {
        setSelectedFeedbackTaskId(0)
      }
      setSelectedFeedbackTaskDetail(null)
      setSelectedFeedbackTaskError('')
      return
    }
    if (selectedFeedbackCorrection.task_id !== selectedFeedbackTaskId) {
      setSelectedFeedbackTaskId(selectedFeedbackCorrection.task_id)
    }
  }, [selectedFeedbackCorrection, selectedFeedbackTaskId])

  useEffect(() => {
    if (activeTab !== 'feedback') {
      return
    }
    const taskId = selectedFeedbackCorrection?.task_id
    if (!taskId) {
      setSelectedFeedbackTaskDetail(null)
      setSelectedFeedbackTaskError('')
      return
    }

    let cancelled = false
    setSelectedFeedbackTaskLoading(true)
    setSelectedFeedbackTaskError('')

    void getMemoryFeedbackCorrection(taskId)
      .then((payload) => {
        if (cancelled) {
          return
        }
        if (!payload.success || !payload.task) {
          setSelectedFeedbackTaskDetail(null)
          setSelectedFeedbackTaskError(payload.error || '未能加载纠错任务详情')
          return
        }
        setSelectedFeedbackTaskDetail(payload.task)
      })
      .catch((error) => {
        if (cancelled) {
          return
        }
        setSelectedFeedbackTaskDetail(null)
        setSelectedFeedbackTaskError(error instanceof Error ? error.message : '未能加载纠错任务详情')
      })
      .finally(() => {
        if (!cancelled) {
          setSelectedFeedbackTaskLoading(false)
        }
      })

    return () => {
      cancelled = true
    }
  }, [activeTab, selectedFeedbackCorrection?.task_id])

  const selectedFeedbackResolved = useMemo<MemoryFeedbackCorrectionDetailTaskPayload | null>(() => {
    if (!selectedFeedbackCorrection) {
      return null
    }
    if (selectedFeedbackTaskDetail?.task_id === selectedFeedbackCorrection.task_id) {
      return {
        ...selectedFeedbackCorrection,
        ...selectedFeedbackTaskDetail,
      } satisfies MemoryFeedbackCorrectionDetailTaskPayload
    }
    return selectedFeedbackTaskDetail ?? selectedFeedbackCorrection
  }, [selectedFeedbackCorrection, selectedFeedbackTaskDetail])
  const selectedFeedbackPreview = useMemo(
    () => getFeedbackCorrectionPreview(selectedFeedbackResolved),
    [selectedFeedbackResolved],
  )
  const selectedFeedbackImpactSummary = useMemo(
    () => buildFeedbackImpactSummary(selectedFeedbackResolved),
    [selectedFeedbackResolved],
  )

  const selectedFeedbackActionLogs: MemoryFeedbackActionLogPayload[] = Array.isArray(selectedFeedbackResolved?.action_logs)
    ? selectedFeedbackResolved.action_logs
    : []
  const filteredFeedbackActionLogs = useMemo(() => {
    const keyword = feedbackActionLogSearch.trim().toLowerCase()
    if (!keyword) {
      return selectedFeedbackActionLogs
    }
    return selectedFeedbackActionLogs.filter((item) =>
      [
        item.action_type,
        item.target_hash,
        item.reason,
        summarizeFeedbackActionPayload(item.before_payload),
        summarizeFeedbackActionPayload(item.after_payload),
      ]
        .map((value) => String(value ?? '').toLowerCase())
        .some((value) => value.includes(keyword)),
    )
  }, [feedbackActionLogSearch, selectedFeedbackActionLogs])
  const feedbackActionLogPageCount = Math.max(
    1,
    Math.ceil(filteredFeedbackActionLogs.length / FEEDBACK_ACTION_LOG_PAGE_SIZE),
  )
  const pagedFeedbackActionLogs = useMemo(() => {
    const start = (feedbackActionLogPage - 1) * FEEDBACK_ACTION_LOG_PAGE_SIZE
    return filteredFeedbackActionLogs.slice(start, start + FEEDBACK_ACTION_LOG_PAGE_SIZE)
  }, [feedbackActionLogPage, filteredFeedbackActionLogs])

  useEffect(() => {
    setFeedbackActionLogPage(1)
  }, [selectedFeedbackTaskId, feedbackActionLogSearch])

  useEffect(() => {
    if (feedbackActionLogPage > feedbackActionLogPageCount) {
      setFeedbackActionLogPage(feedbackActionLogPageCount)
    }
  }, [feedbackActionLogPage, feedbackActionLogPageCount])

  const openFeedbackRollbackDialog = useCallback(() => {
    setFeedbackRollbackReason('')
    setFeedbackRollbackDialogOpen(true)
  }, [])

  const executeFeedbackRollback = useCallback(async () => {
    const taskId = selectedFeedbackResolved?.task_id
    if (!taskId) {
      return
    }
    try {
      setFeedbackRollingBack(true)
      const payload = await rollbackMemoryFeedbackCorrection(taskId, {
        requested_by: 'knowledge_base',
        reason: feedbackRollbackReason.trim(),
      })
      if (!payload.success) {
        throw new Error(payload.error || '回退失败')
      }
      toast({
        title: payload.already_rolled_back ? '该纠错已回退' : '纠错回退成功',
        description: `任务 ${taskId} 的回退结果已写入日志`,
      })
      setFeedbackRollbackDialogOpen(false)
      const [listPayload, detailPayload] = await Promise.all([
        getMemoryFeedbackCorrections({ limit: FEEDBACK_CORRECTION_FETCH_LIMIT }),
        getMemoryFeedbackCorrection(taskId),
      ])
      setFeedbackCorrections(listPayload.items ?? [])
      setSelectedFeedbackTaskDetail(detailPayload.task ?? null)
      const [sourcePayload, runtimePayload] = await Promise.all([
        getMemorySources(),
        getMemoryRuntimeConfig(),
      ])
      setMemorySources(sourcePayload.items ?? [])
      setRuntimeConfig(runtimePayload)
    } catch (error) {
      toast({
        title: '纠错回退失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setFeedbackRollingBack(false)
    }
  }, [feedbackRollbackReason, selectedFeedbackResolved?.task_id, toast])

  const selectedOperationResolved = useMemo(() => {
    if (!selectedDeleteOperation) {
      return null
    }
    if (selectedOperationDetail?.operation_id === selectedDeleteOperation.operation_id) {
      return {
        ...selectedDeleteOperation,
        ...selectedOperationDetail,
      } satisfies MemoryDeleteOperationPayload
    }
    return selectedDeleteOperation
  }, [selectedDeleteOperation, selectedOperationDetail])
  const selectedOperationSummaryResolved = ((selectedOperationResolved?.summary ?? {}) as Record<string, unknown>)
  const selectedOperationCounts = ((selectedOperationSummaryResolved.counts as Record<string, number> | undefined) ?? {})
  const selectedOperationSources = Array.isArray(selectedOperationSummaryResolved.sources)
    ? selectedOperationSummaryResolved.sources.map((item) => String(item)).filter(Boolean)
    : []
  const selectedOperationItems = Array.isArray(selectedOperationResolved?.items)
    ? selectedOperationResolved.items
    : []
  const filteredSelectedOperationItems = useMemo(() => {
    const keyword = selectedOperationItemSearch.trim().toLowerCase()
    if (!keyword) {
      return selectedOperationItems
    }
    return selectedOperationItems.filter((item) => {
      const payload = item.payload ?? {}
      const source = String(payload.source ?? '').trim()
      return [
        item.item_type,
        item.item_hash,
        item.item_key,
        source,
      ]
        .map((value) => String(value ?? '').toLowerCase())
        .some((value) => value.includes(keyword))
    })
  }, [selectedOperationItemSearch, selectedOperationItems])
  const selectedOperationItemPageCount = Math.max(
    1,
    Math.ceil(filteredSelectedOperationItems.length / DELETE_OPERATION_ITEM_PAGE_SIZE),
  )
  const pagedSelectedOperationItems = useMemo(() => {
    const start = (selectedOperationItemPage - 1) * DELETE_OPERATION_ITEM_PAGE_SIZE
    return filteredSelectedOperationItems.slice(start, start + DELETE_OPERATION_ITEM_PAGE_SIZE)
  }, [filteredSelectedOperationItems, selectedOperationItemPage])

  useEffect(() => {
    setSelectedOperationItemPage(1)
  }, [selectedOperationId, selectedOperationItemSearch])

  useEffect(() => {
    if (selectedOperationItemPage > selectedOperationItemPageCount) {
      setSelectedOperationItemPage(selectedOperationItemPageCount)
    }
  }, [selectedOperationItemPage, selectedOperationItemPageCount])

  const refreshSelfCheck = useCallback(async () => {
    try {
      setRefreshingCheck(true)
      const payload = await refreshMemoryRuntimeSelfCheck()
      const nextRuntime = await getMemoryRuntimeConfig()
      setRuntimeConfig(nextRuntime)
      toast({
        title: payload.success ? '自检通过' : '自检未通过',
        description: payload.success ? '运行时状态正常' : '请检查 embedding 配置和外部服务连通性',
        variant: payload.success ? 'default' : 'destructive',
      })
    } catch (error) {
      toast({
        title: '运行时自检失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setRefreshingCheck(false)
    }
  }, [toast])

  const openVectorRebuildDialog = useCallback(async () => {
    try {
      setVectorRebuildDialogOpen(true)
      setVectorRebuildPreview(null)
      const payload = await rebuildMemoryRuntimeVectors({ dry_run: true })
      setVectorRebuildPreview(payload.counts ?? null)
    } catch (error) {
      toast({
        title: '读取向量重建预览失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    }
  }, [toast])

  const confirmVectorRebuild = useCallback(async () => {
    try {
      setVectorRebuilding(true)
      const payload = await rebuildMemoryRuntimeVectors({ dry_run: false })
      const nextRuntime = await getMemoryRuntimeConfig()
      setRuntimeConfig(nextRuntime)
      setVectorRebuildDialogOpen(false)
      toast({
        title: payload.success ? '向量重建完成' : '向量重建未完全成功',
        description: `已处理 ${payload.done ?? 0} 条，失败 ${payload.failed ?? 0} 条`,
        variant: payload.success ? 'default' : 'destructive',
      })
    } catch (error) {
      toast({
        title: '向量重建失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setVectorRebuilding(false)
    }
  }, [toast])

  const submitImportByMode = useCallback(async () => {
    if (creatingImport) {
      return
    }
    switch (importCreateMode) {
      case 'upload':
        await submitUploadImport()
        break
      case 'paste':
        await submitPasteImport()
        break
      case 'raw_scan':
        await submitRawScanImport()
        break
      case 'lpmm_openie':
        await submitOpenieImport()
        break
      case 'lpmm_convert':
        await submitConvertImport()
        break
      case 'temporal_backfill':
        await submitBackfillImport()
        break
      case 'maibot_migration':
        await submitMaibotMigrationImport()
        break
      default:
        break
    }
  }, [
    creatingImport,
    importCreateMode,
    submitBackfillImport,
    submitConvertImport,
    submitMaibotMigrationImport,
    submitOpenieImport,
    submitPasteImport,
    submitRawScanImport,
    submitUploadImport,
  ])

  const submitTuningTask = useCallback(async () => {
    try {
      setCreatingTuning(true)
      await createMemoryTuningTask({
        objective: tuningObjective,
        intensity: tuningIntensity,
        sample_size: Number(tuningSampleSize),
        top_k_eval: Number(tuningTopKEval),
      })
      const tasks = await getMemoryTuningTasks(20)
      setTuningTasks(tasks.items ?? [])
      toast({ title: '调优任务已创建', description: '新的检索调优任务已经进入队列' })
    } catch (error) {
      toast({
        title: '创建调优任务失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    } finally {
      setCreatingTuning(false)
    }
  }, [toast, tuningIntensity, tuningObjective, tuningSampleSize, tuningTopKEval])

  const applyBestTask = useCallback(async (taskId: string) => {
    try {
      await applyBestMemoryTuningProfile(taskId)
      const [profilePayload, runtimePayload, tuningTaskPayload] = await Promise.all([
        getMemoryTuningProfile(),
        getMemoryRuntimeConfig(),
        getMemoryTuningTasks(20),
      ])
      setTuningProfile(profilePayload.profile ?? {})
      setTuningProfileToml(profilePayload.toml ?? '')
      setRuntimeConfig(runtimePayload)
      setTuningTasks(tuningTaskPayload.items ?? [])
      toast({ title: '最佳参数已应用', description: `任务 ${taskId} 的最佳轮次已经写入运行时` })
    } catch (error) {
      toast({
        title: '应用最佳参数失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive',
      })
    }
  }, [toast])

  const dismissQuickStart = useCallback(() => {
    window.localStorage.setItem(MEMORY_QUICK_START_DISMISSED_KEY, 'true')
    setQuickStartVisible(false)
  }, [])

  const shouldRenderMemoryTab = (tab: MemoryConsoleTab) => activeTab === tab || visitedMemoryTabs.has(tab)
  const shouldShowPanelFallback = (tab: LoadableMemoryTab) => !loadedPanelDataRef.current.has(tab)
  const renderPanelFallback = (tab: LoadableMemoryTab) => (
    <TabsContent value={tab} className="space-y-4">
      <div className="flex min-h-[240px] items-center justify-center rounded-xl border bg-background/70 text-sm text-muted-foreground">
        <ThinkingIllustration size={tabLoading[tab] ? 'md' : 'sm'} />
      </div>
    </TabsContent>
  )

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="rounded-xl border bg-background px-6 py-4 shadow-sm">
          <ThinkingIllustration size="lg" />
        </div>
      </div>
    )
  }

  return (
    <div className="flex h-full flex-col bg-background">
      <div className="flex-1 overflow-auto">
        <div className="memory-console-density mx-auto flex w-full max-w-[1800px] flex-col gap-4 px-4 py-4 xl:px-5">
          <div className="hidden">
            <Button variant="outline" size="sm" onClick={() => void loadPage()}>
              <RefreshCw className="mr-2 h-4 w-4" />
              刷新数据
            </Button>
          </div>
          {/* 运行时状态条 —— 紧凑、常驻、一眼看完 */}
          {runtimeBadges.length > 0 ? (
            <div className="rounded-xl border border-border/60 bg-card/60 p-3 shadow-sm backdrop-blur">
              <div className="mb-2 flex items-center justify-end gap-2">
                {runtimeConfig?.vector_rebuild_required ? (
                  <Button
                    variant="destructive"
                    size="sm"
                    className="h-6 px-2 text-[11px]"
                    onClick={() => void openVectorRebuildDialog()}
                    disabled={vectorRebuilding}
                  >
                    <RotateCcw className={cn('mr-1 h-3 w-3', vectorRebuilding && 'animate-spin')} />
                    重建向量
                  </Button>
                ) : null}
                <Button
                  variant="outline"
                  size="sm"
                  className="h-6 px-2 text-[11px]"
                  onClick={() => void loadPage()}
                >
                  <RefreshCw className="mr-1 h-3 w-3" />
                  刷新数据
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 px-2 text-[11px]"
                  onClick={() => void refreshSelfCheck()}
                  disabled={refreshingCheck}
                >
                  <RefreshCw className={cn('mr-1 h-3 w-3', refreshingCheck && 'animate-spin')} />
                  自检
                </Button>
              </div>
              <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                {runtimeBadges.map((item) => (
                  <div
                    key={item.label}
                    className={cn(
                      'flex items-center gap-2 rounded-lg border px-2.5 py-1.5 transition-colors',
                      item.className,
                    )}
                  >
                    <div className="flex-none rounded-md border bg-background/70 p-1 shadow-sm">
                      <item.icon className={cn('h-3.5 w-3.5', item.iconClassName)} />
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-[10px] font-medium leading-tight text-muted-foreground">
                        {item.label}
                      </div>
                      <div className="truncate text-xs font-semibold leading-tight" title={item.value}>
                        {item.value}
                      </div>
                      <div className="mt-0.5 hidden truncate text-[10px] text-muted-foreground xl:block">
                        {item.description}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          <Dialog open={vectorRebuildDialogOpen} onOpenChange={setVectorRebuildDialogOpen}>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>重建全部向量</DialogTitle>
                <DialogDescription>
                  将使用当前 embedding 配置重新生成段落、实体和已启用的关系向量，期间检索会临时降级（会对嵌入模型造成大量请求！）
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-3 text-sm">
                <Alert variant={runtimeConfig?.vector_rebuild_required ? 'destructive' : 'default'}>
                  <AlertDescription>
                    {runtimeConfig?.vector_rebuild_message || '这个操作会替换现有向量库，适合更换 embedding 模型或维度后执行。'}
                  </AlertDescription>
                </Alert>
                <div className="grid gap-2 sm:grid-cols-3">
                  {(['paragraphs', 'entities', 'relations'] as const).map((key) => (
                    <div key={key} className="rounded-lg border bg-muted/30 p-3">
                      <div className="text-xs text-muted-foreground">
                        {key === 'paragraphs' ? '段落' : key === 'entities' ? '实体' : '关系'}
                      </div>
                      <div className="mt-1 text-xl font-semibold">{vectorRebuildPreview?.[key] ?? '-'}</div>
                    </div>
                  ))}
                </div>
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={() => setVectorRebuildDialogOpen(false)} disabled={vectorRebuilding}>
                  取消
                </Button>
                <Button variant="destructive" onClick={() => void confirmVectorRebuild()} disabled={vectorRebuilding}>
                  <RotateCcw className={cn('mr-2 h-4 w-4', vectorRebuilding && 'animate-spin')} />
                  确认重建
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>

          {/* 快速开始 Hero —— 给新用户明确的"先做什么" */}
          {quickStartVisible && (
            <div className="relative overflow-hidden rounded-xl border border-primary/20 bg-gradient-to-br from-primary/10 via-primary/5 to-transparent p-4 pr-11 shadow-sm">
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="absolute right-3 top-3 h-7 w-7 text-muted-foreground hover:text-foreground"
                onClick={dismissQuickStart}
                aria-label="关闭快速开始"
                title="关闭快速开始"
              >
                <X className="h-4 w-4" />
              </Button>
              <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="space-y-1.5 lg:max-w-sm">
                <div className="text-[11px] font-medium uppercase tracking-[0.18em] text-primary">
                  快速开始
                </div>
                <h2 className="text-lg font-semibold leading-tight">先从这三件事入手</h2>
                <p className="text-sm text-muted-foreground">
                  不知道该做什么？挑一个最常用的入口，下面的标签页里有更详细的设置。
                </p>
              </div>
              <div className="grid w-full gap-2 sm:grid-cols-3 lg:max-w-3xl">
                <button
                  type="button"
                  onClick={() => switchMemoryTab('import')}
                  className="group flex items-start gap-2 rounded-lg border border-border/70 bg-background/80 p-3 text-left transition hover:border-primary/50 hover:bg-background hover:shadow-md"
                >
                  <div className="flex-none rounded-lg bg-primary/10 p-2 text-primary transition-transform group-hover:scale-105">
                    <Upload className="h-4 w-4" />
                  </div>
                  <div className="min-w-0">
                    <div className="text-sm font-semibold">导入资料</div>
                    <div className="mt-0.5 text-xs leading-relaxed text-muted-foreground">
                      把文件、聊天记录写进记忆库
                    </div>
                  </div>
                </button>
                <button
                  type="button"
                  onClick={() => switchMemoryTab('tuning')}
                  className="group flex items-start gap-2 rounded-lg border border-border/70 bg-background/80 p-3 text-left transition hover:border-primary/50 hover:bg-background hover:shadow-md"
                >
                  <div className="flex-none rounded-lg bg-amber-500/10 p-2 text-amber-500 transition-transform group-hover:scale-105">
                    <SlidersHorizontal className="h-4 w-4" />
                  </div>
                  <div className="min-w-0">
                    <div className="text-sm font-semibold">检索调优</div>
                    <div className="mt-0.5 text-xs leading-relaxed text-muted-foreground">
                      让回忆变得更准、更聪明
                    </div>
                  </div>
                </button>
                <button
                  type="button"
                  onClick={() => switchMemoryTab('graph')}
                  className="group flex items-start gap-2 rounded-lg border border-border/70 bg-background/80 p-3 text-left transition hover:border-primary/50 hover:bg-background hover:shadow-md"
                >
                  <div className="flex-none rounded-lg bg-violet-500/10 p-2 text-violet-500 transition-transform group-hover:scale-105">
                    <Database className="h-4 w-4" />
                  </div>
                  <div className="min-w-0">
                    <div className="text-sm font-semibold">打开图谱</div>
                    <div className="mt-0.5 text-xs leading-relaxed text-muted-foreground">
                      可视化已存的实体和关系
                    </div>
                  </div>
                </button>
              </div>
              </div>
            </div>
          )}

          <Tabs
            value={activeTab}
            onValueChange={(value) => switchMemoryTab(value as MemoryConsoleTab)}
            className="space-y-3"
          >
            <div className="-mx-4 border-b border-border/40 px-4 pb-1.5 pt-0 xl:-mx-5 xl:px-5">
              <div className="flex flex-wrap items-center gap-2">
                <MemoryMiniTabs
                  items={[
                    { value: 'graph', label: '图谱', description: '实体关系图与证据视图' },
                    { value: 'timeline', label: '审计时间线', description: '核对聊天流记忆变动' },
                    { value: 'tuning', label: '调优', description: '检索策略调优' },
                    { value: 'episodes', label: '情景记忆', description: '查看和重建情景记忆' },
                    { value: 'profiles', label: '人物画像', description: '查询和维护人物画像' },
                  ]}
                  className="w-fit max-w-full"
                  triggerClassName="px-3"
                />
                <MemoryMiniTabs
                  items={[
                    { value: 'import', label: '导入', description: '创建并管理导入任务' },
                    { value: 'maintenance', label: '维护', description: '回收站与记忆状态维护' },
                    { value: 'delete', label: '删除', description: '批量删除与历史回溯' },
                    { value: 'feedback', label: '纠错历史', description: '查看反馈与回滚' },
                  ]}
                  className="w-fit max-w-full"
                  triggerClassName="px-3"
                />
              </div>
            </div>

            <TabsContent value="graph" className="h-[calc(100vh-132px)] min-h-[820px] overflow-hidden rounded-2xl border border-border/60 bg-background shadow-sm">
              <KnowledgeGraphPage embedded onOpenConsole={() => switchMemoryTab('import')} />
            </TabsContent>

            {shouldRenderMemoryTab('timeline') && (shouldShowPanelFallback('timeline') ? renderPanelFallback('timeline') : (
            <TabsContent value="timeline" className="space-y-4">
              <MemoryTimelineManager
                chatTargets={importChatTargets}
                initialChatId={timelineInitialChatId}
                initialTimeStart={timelineInitialTimeStart}
                initialTimeEnd={timelineInitialTimeEnd}
                onJump={handleTimelineJump}
              />
            </TabsContent>
            ))}

            {shouldRenderMemoryTab('import') && (shouldShowPanelFallback('import') ? renderPanelFallback('import') : (
            <ImportTab
              importCreateMode={importCreateMode}
              setImportCreateMode={setImportCreateMode}
              importSettings={importSettings}
              importCommonFileConcurrency={importCommonFileConcurrency}
              setImportCommonFileConcurrency={setImportCommonFileConcurrency}
              importCommonChunkConcurrency={importCommonChunkConcurrency}
              setImportCommonChunkConcurrency={setImportCommonChunkConcurrency}
              importCommonNarrativeWindowSize={importCommonNarrativeWindowSize}
              setImportCommonNarrativeWindowSize={setImportCommonNarrativeWindowSize}
              importCommonNarrativeOverlap={importCommonNarrativeOverlap}
              setImportCommonNarrativeOverlap={setImportCommonNarrativeOverlap}
              importCommonFactualTargetSize={importCommonFactualTargetSize}
              setImportCommonFactualTargetSize={setImportCommonFactualTargetSize}
              importCommonLlmEnabled={importCommonLlmEnabled}
              setImportCommonLlmEnabled={setImportCommonLlmEnabled}
              importCommonChatLog={importCommonChatLog}
              setImportCommonChatLog={setImportCommonChatLog}
              importCommonChatId={importCommonChatId}
              setImportCommonChatId={setImportCommonChatId}
              importChatTargets={importChatTargets}
              importCommonStrategyOverride={importCommonStrategyOverride}
              setImportCommonStrategyOverride={setImportCommonStrategyOverride}
              importCommonDedupePolicy={importCommonDedupePolicy}
              setImportCommonDedupePolicy={setImportCommonDedupePolicy}
              importCommonChatReferenceTime={importCommonChatReferenceTime}
              setImportCommonChatReferenceTime={setImportCommonChatReferenceTime}
              importCommonForce={importCommonForce}
              setImportCommonForce={setImportCommonForce}
              importCommonClearManifest={importCommonClearManifest}
              setImportCommonClearManifest={setImportCommonClearManifest}
              uploadInputMode={uploadInputMode}
              setUploadInputMode={setUploadInputMode}
              uploadFiles={uploadFiles}
              setUploadFiles={setUploadFiles}
              pasteName={pasteName}
              setPasteName={setPasteName}
              pasteMode={pasteMode}
              setPasteMode={setPasteMode}
              pasteContent={pasteContent}
              setPasteContent={setPasteContent}
              rawAlias={rawAlias}
              setRawAlias={setRawAlias}
              rawInputMode={rawInputMode}
              setRawInputMode={setRawInputMode}
              rawRelativePath={rawRelativePath}
              setRawRelativePath={setRawRelativePath}
              rawGlob={rawGlob}
              setRawGlob={setRawGlob}
              rawRecursive={rawRecursive}
              setRawRecursive={setRawRecursive}
              openieAlias={openieAlias}
              setOpenieAlias={setOpenieAlias}
              openieRelativePath={openieRelativePath}
              setOpenieRelativePath={setOpenieRelativePath}
              openieIncludeAllJson={openieIncludeAllJson}
              setOpenieIncludeAllJson={setOpenieIncludeAllJson}
              convertAlias={convertAlias}
              setConvertAlias={setConvertAlias}
              convertTargetAlias={convertTargetAlias}
              setConvertTargetAlias={setConvertTargetAlias}
              convertRelativePath={convertRelativePath}
              setConvertRelativePath={setConvertRelativePath}
              convertTargetRelativePath={convertTargetRelativePath}
              setConvertTargetRelativePath={setConvertTargetRelativePath}
              convertDimension={convertDimension}
              setConvertDimension={setConvertDimension}
              convertBatchSize={convertBatchSize}
              setConvertBatchSize={setConvertBatchSize}
              backfillAlias={backfillAlias}
              setBackfillAlias={setBackfillAlias}
              backfillLimit={backfillLimit}
              setBackfillLimit={setBackfillLimit}
              backfillRelativePath={backfillRelativePath}
              setBackfillRelativePath={setBackfillRelativePath}
              backfillDryRun={backfillDryRun}
              setBackfillDryRun={setBackfillDryRun}
              backfillNoCreatedFallback={backfillNoCreatedFallback}
              setBackfillNoCreatedFallback={setBackfillNoCreatedFallback}
              maibotSourceDb={maibotSourceDb}
              setMaibotSourceDb={setMaibotSourceDb}
              maibotTimeFrom={maibotTimeFrom}
              setMaibotTimeFrom={setMaibotTimeFrom}
              maibotTimeTo={maibotTimeTo}
              setMaibotTimeTo={setMaibotTimeTo}
              maibotStartId={maibotStartId}
              setMaibotStartId={setMaibotStartId}
              maibotEndId={maibotEndId}
              setMaibotEndId={setMaibotEndId}
              maibotStreamIds={maibotStreamIds}
              setMaibotStreamIds={setMaibotStreamIds}
              maibotGroupIds={maibotGroupIds}
              setMaibotGroupIds={setMaibotGroupIds}
              maibotUserIds={maibotUserIds}
              setMaibotUserIds={setMaibotUserIds}
              maibotReadBatchSize={maibotReadBatchSize}
              setMaibotReadBatchSize={setMaibotReadBatchSize}
              maibotCommitWindowRows={maibotCommitWindowRows}
              setMaibotCommitWindowRows={setMaibotCommitWindowRows}
              maibotEmbedWorkers={maibotEmbedWorkers}
              setMaibotEmbedWorkers={setMaibotEmbedWorkers}
              maibotNoResume={maibotNoResume}
              setMaibotNoResume={setMaibotNoResume}
              maibotResetState={maibotResetState}
              setMaibotResetState={setMaibotResetState}
              maibotDryRun={maibotDryRun}
              setMaibotDryRun={setMaibotDryRun}
              maibotVerifyOnly={maibotVerifyOnly}
              setMaibotVerifyOnly={setMaibotVerifyOnly}
              submitImportByMode={submitImportByMode}
              creatingImport={creatingImport}
              pathResolveAlias={pathResolveAlias}
              setPathResolveAlias={setPathResolveAlias}
              importAliasKeys={importAliasKeys}
              pathResolveRelativePath={pathResolveRelativePath}
              setPathResolveRelativePath={setPathResolveRelativePath}
              pathResolveMustExist={pathResolveMustExist}
              setPathResolveMustExist={setPathResolveMustExist}
              resolveImportPath={resolveImportPath}
              resolvingPath={resolvingPath}
              pathResolveOutput={pathResolveOutput}
              refreshImportQueue={refreshImportQueue}
              runningImportTasks={runningImportTasks}
              queuedImportTasks={queuedImportTasks}
              recentImportTasks={recentImportTasks}
              selectedImportTaskId={selectedImportTaskId}
              selectImportTask={selectImportTask}
              importAutoPolling={importAutoPolling}
              setImportAutoPolling={setImportAutoPolling}
              importPollInterval={importPollInterval}
              importErrorText={importErrorText}
              cancelSelectedImportTask={cancelSelectedImportTask}
              retrySelectedImportTask={retrySelectedImportTask}
              selectedImportTaskLoading={selectedImportTaskLoading}
              selectedImportTaskResolved={selectedImportTaskResolved}
              selectedImportRetrySummary={selectedImportRetrySummary}
              selectedImportTaskErrorText={selectedImportTaskErrorText}
              selectedImportFiles={selectedImportFiles}
              selectedImportFileId={selectedImportFileId}
              selectImportFile={selectImportFile}
              importChunkTotal={importChunkTotal}
              importChunkOffset={importChunkOffset}
              moveImportChunkPage={moveImportChunkPage}
              canImportChunkPrev={canImportChunkPrev}
              canImportChunkNext={canImportChunkNext}
              importChunksLoading={importChunksLoading}
              selectedImportChunks={selectedImportChunks}
            />
            ))}

            {shouldRenderMemoryTab('tuning') && (shouldShowPanelFallback('tuning') ? renderPanelFallback('tuning') : (
            <TuningTab
              tuningObjective={tuningObjective}
              setTuningObjective={setTuningObjective}
              tuningIntensity={tuningIntensity}
              setTuningIntensity={setTuningIntensity}
              tuningSampleSize={tuningSampleSize}
              setTuningSampleSize={setTuningSampleSize}
              tuningTopKEval={tuningTopKEval}
              setTuningTopKEval={setTuningTopKEval}
              submitTuningTask={submitTuningTask}
              creatingTuning={creatingTuning}
              tuningProfile={tuningProfile}
              tuningProfileToml={tuningProfileToml}
              tuningTasks={tuningTasks}
              applyBestTask={applyBestTask}
            />
            ))}

            <TabsContent value="episodes" className="space-y-4">
              {shouldRenderMemoryTab('episodes') ? (
                <MemoryEpisodeManager
                  initialEpisodeId={episodeInitialTarget.episodeId}
                  initialSource={episodeInitialTarget.source}
                  initialTimeStart={episodeInitialTarget.timeStart}
                  initialTimeEnd={episodeInitialTarget.timeEnd}
                />
              ) : null}
            </TabsContent>

            <TabsContent value="profiles" className="space-y-4">
              {shouldRenderMemoryTab('profiles') ? <MemoryProfileManager initialPersonId={profileInitialPersonId} /> : null}
            </TabsContent>

            <TabsContent value="maintenance" className="space-y-4">
              {shouldRenderMemoryTab('maintenance') ? <MemoryMaintenanceManager initialTarget={maintenanceInitialTarget} /> : null}
            </TabsContent>

            {shouldRenderMemoryTab('delete') && (shouldShowPanelFallback('delete') ? renderPanelFallback('delete') : (
            <DeleteTab
              sourceSearch={sourceSearch}
              setSourceSearch={setSourceSearch}
              selectedSources={selectedSources}
              setSelectedSources={setSelectedSources}
              filteredSources={filteredSources}
              openSourceDeletePreview={openSourceDeletePreview}
              toggleSourceSelection={toggleSourceSelection}
              operationSearch={operationSearch}
              setOperationSearch={setOperationSearch}
              operationModeFilter={operationModeFilter}
              setOperationModeFilter={setOperationModeFilter}
              operationStatusFilter={operationStatusFilter}
              setOperationStatusFilter={setOperationStatusFilter}
              filteredDeleteOperations={filteredDeleteOperations}
              deleteOperations={deleteOperations}
              operationPage={operationPage}
              setOperationPage={setOperationPage}
              deleteOperationPageCount={deleteOperationPageCount}
              pagedDeleteOperations={pagedDeleteOperations}
              selectedDeleteOperation={selectedDeleteOperation}
              setSelectedOperationId={setSelectedOperationId}
              restoreDeleteOperation={restoreDeleteOperation}
              deleteRestoring={deleteRestoring}
              selectedOperationCounts={selectedOperationCounts}
              selectedOperationDetailLoading={selectedOperationDetailLoading}
              selectedOperationDetailError={selectedOperationDetailError}
              selectedOperationSources={selectedOperationSources}
              selectedOperationItems={selectedOperationItems}
              filteredSelectedOperationItems={filteredSelectedOperationItems}
              selectedOperationItemSearch={selectedOperationItemSearch}
              setSelectedOperationItemSearch={setSelectedOperationItemSearch}
              selectedOperationItemPage={selectedOperationItemPage}
              setSelectedOperationItemPage={setSelectedOperationItemPage}
              selectedOperationItemPageCount={selectedOperationItemPageCount}
              pagedSelectedOperationItems={pagedSelectedOperationItems}
            />
            ))}

            {shouldRenderMemoryTab('feedback') && (shouldShowPanelFallback('feedback') ? renderPanelFallback('feedback') : (
            <FeedbackTab
              feedbackSearch={feedbackSearch}
              setFeedbackSearch={setFeedbackSearch}
              feedbackStatusFilter={feedbackStatusFilter}
              setFeedbackStatusFilter={setFeedbackStatusFilter}
              feedbackRollbackFilter={feedbackRollbackFilter}
              setFeedbackRollbackFilter={setFeedbackRollbackFilter}
              filteredFeedbackCorrections={filteredFeedbackCorrections}
              feedbackCorrections={feedbackCorrections}
              pagedFeedbackCorrections={pagedFeedbackCorrections}
              feedbackPage={feedbackPage}
              setFeedbackPage={setFeedbackPage}
              feedbackPageCount={feedbackPageCount}
              selectedFeedbackCorrection={selectedFeedbackCorrection}
              setSelectedFeedbackTaskId={setSelectedFeedbackTaskId}
              selectedFeedbackResolved={selectedFeedbackResolved}
              selectedFeedbackPreview={selectedFeedbackPreview}
              selectedFeedbackImpactSummary={selectedFeedbackImpactSummary}
              openFeedbackRollbackDialog={openFeedbackRollbackDialog}
              feedbackRollingBack={feedbackRollingBack}
              selectedFeedbackTaskLoading={selectedFeedbackTaskLoading}
              selectedFeedbackTaskError={selectedFeedbackTaskError}
              feedbackActionLogPage={feedbackActionLogPage}
              setFeedbackActionLogPage={setFeedbackActionLogPage}
              feedbackActionLogPageCount={feedbackActionLogPageCount}
              feedbackActionLogSearch={feedbackActionLogSearch}
              setFeedbackActionLogSearch={setFeedbackActionLogSearch}
              pagedFeedbackActionLogs={pagedFeedbackActionLogs}
              selectedFeedbackActionLogs={selectedFeedbackActionLogs}
            />
            ))}
          </Tabs>
        </div>
      </div>

      <MemoryDeleteDialog
        open={deleteDialogOpen}
        onOpenChange={closeDeleteDialog}
        title={deleteDialogTitle}
        description={deleteDialogDescription}
        preview={deletePreview}
        result={deleteResult}
        loadingPreview={deletePreviewLoading}
        executing={deleteExecuting}
        restoring={deleteRestoring}
        error={deletePreviewError}
        onExecute={() => void executePendingDelete()}
        onRestore={() => void (deleteResult?.operation_id ? restoreDeleteOperation(deleteResult.operation_id) : Promise.resolve())}
      />

      <Dialog open={feedbackRollbackDialogOpen} onOpenChange={setFeedbackRollbackDialogOpen}>
        <DialogContent className="max-w-lg" confirmOnEnter>
          <DialogHeader>
            <DialogTitle>回退本次纠错</DialogTitle>
            <DialogDescription>
              这会恢复旧关系状态、隐藏本次纠错写入的段落，并重新触发 Episode / Profile 的异步修复。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div className="rounded-lg border bg-muted/20 p-3 text-sm">
              <div className="font-medium break-words">{selectedFeedbackResolved?.query_text || '无查询文本'}</div>
              <div className="mt-1 font-mono text-[11px] break-all text-muted-foreground">
                {selectedFeedbackResolved?.query_tool_id}
              </div>
            </div>
            <div className="space-y-2">
              <Label htmlFor="feedback-rollback-reason">回退原因</Label>
              <Textarea
                id="feedback-rollback-reason"
                value={feedbackRollbackReason}
                onChange={(event) => setFeedbackRollbackReason(event.target.value)}
                placeholder="可选，建议填写本次人工回退原因"
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setFeedbackRollbackDialogOpen(false)} disabled={feedbackRollingBack}>
              取消
            </Button>
            <Button onClick={() => void executeFeedbackRollback()} disabled={feedbackRollingBack}>
              {feedbackRollingBack ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  回退中
                </>
              ) : (
                <>
                  <RotateCcw className="mr-2 h-4 w-4" />
                  确认回退
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
