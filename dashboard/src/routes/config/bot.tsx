import { Fragment, lazy, type ReactNode, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate, useRouterState } from '@tanstack/react-router'
import {
  Check,
  ChevronRight,
  Code2,
  Info,
  MoreHorizontal,
  RefreshCw,
  Save,
  Settings,
  ShieldCheck,
  SlidersHorizontal,
  UsersRound,
  X,
} from 'lucide-react'
import { parse as parseToml } from 'smol-toml'

import { Alert, AlertDescription } from '@/components/ui/alert'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { DashboardTabBar, DashboardTabTrigger } from '@/components/ui/dashboard-tabs'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ThinkingIllustration } from '@/components/ui/thinking-illustration'
import { CodeEditor } from '@/components/CodeEditor'
import { DynamicConfigForm } from '@/components/dynamic-form'
import { RestartOverlay } from '@/components/restart-overlay'
import { useToast } from '@/hooks/use-toast'
import {
  getBotConfig,
  getBotConfigCached,
  getBotConfigRaw,
  getBotConfigSchema,
  updateBotConfig,
  updateBotConfigRaw,
} from '@/lib/config-api'
import { fieldHooks } from '@/lib/field-hooks'
import { getConfigSearchField, scrollToConfigSearchField } from '@/lib/config-search-navigation'
import { RestartProvider, useRestart } from '@/lib/restart-context'
import { cn } from '@/lib/utils'
import { SharedGroupsSettings } from '@/routes/chat-management'

import type { ConfigSchema } from '@/types/config-schema'
import {
  AliasNamesHook,
  AMemorixSharedMemoryGroupsHook,
  AMemorixRetrievalChatsHook,
  AMemorixRetrievalFilterGroupHook,
  BehaviorGroupsHook,
  BehaviorFocusGroupsHook,
  BehaviorLearningListHook,
  ChatPromptsHook,
  ChatTalkValueRulesHook,
  ExpressionGroupsHook,
  ExpressionLearningListHook,
  FocusWhitelistHook,
  JargonGroupsHook,
  JargonLearningListHook,
  KeywordRulesHook,
  HiddenFieldHook,
  MCPRootItemsHook,
  MCPServersHook,
  MultipleReplyStyleHook,
  RegexRulesHook,
  useAutoSave,
} from './bot/hooks'
import { CommandPermissions } from './bot/CommandPermissions'
import { GlobalLearningSettings } from './bot/GlobalLearningSettings'

const WebUISettings = lazy(() =>
  import('@/routes/settings').then((module) => ({ default: module.SettingsPage }))
)

type ConfigSectionData = Record<string, unknown>
type BotSettingsMode = 'groups' | 'detail' | 'commands' | 'source' | 'webui'
// ==================== 常量定义 ====================
/** Toast 显示前的延迟时间 (毫秒) */
const TOAST_DISPLAY_DELAY = 500
const FILE_MODE_NOTICE_DISMISSED_KEY = 'bot-config-file-mode-notice-dismissed'
const EXPERIMENTAL_FEATURES_NOTICE_DISMISSED_KEY =
  'bot-config-experimental-features-notice-dismissed'

// ==================== Tab 分组类型与构建 ====================
interface TabGroup {
  id: string
  label: string
  advanced: boolean
  order: number
  sections: string[]
}

interface SubtabPane {
  advanced: boolean
  content: ReactNode
  id: string
  label: string
}

/**
 * 从 schema 的 nested 字段解析出 tab 分组信息。
 * - 有 uiLabel 且无 uiParent → 独立 tab
 * - 有 uiParent → 递归找到最终 host，并归入对应 tab
 */
function buildTabGroupsFromSchema(schema: ConfigSchema): TabGroup[] {
  const nested = schema.nested || {}
  const nestedEntries = Object.entries(nested)
  const hosts = new Map<string, TabGroup>()

  const resolveHostId = (fieldName: string, visited: Set<string> = new Set()): string | null => {
    if (visited.has(fieldName)) {
      return null
    }

    const fieldSchema = nested[fieldName]
    if (!fieldSchema) {
      return null
    }

    if (!fieldSchema.uiParent) {
      return fieldSchema.uiLabel ? fieldName : null
    }

    visited.add(fieldName)
    return resolveHostId(fieldSchema.uiParent, visited)
  }

  for (const [fieldName, fieldSchema] of nestedEntries) {
    if (fieldSchema.uiLabel && !fieldSchema.uiParent) {
      hosts.set(fieldName, {
        id: fieldName,
        label: fieldSchema.uiLabel,
        advanced: fieldName === 'visual' || fieldName === 'expression' || Boolean(fieldSchema.uiAdvanced),
        order: fieldSchema.uiOrder ?? Number.POSITIVE_INFINITY,
        sections: [fieldName],
      })
    }
  }

  for (const [fieldName] of nestedEntries) {
    const hostId = resolveHostId(fieldName)
    if (!hostId || hostId === fieldName) {
      continue
    }

    const parent = hosts.get(hostId)
    if (parent && !parent.sections.includes(fieldName)) {
      parent.sections.push(fieldName)
    }
  }

  return Array.from(hosts.values()).sort((a, b) => {
    const orderDelta = a.order - b.order
    if (orderDelta !== 0) {
      return orderDelta
    }
    return a.label.localeCompare(b.label, 'zh-CN')
  })
}

// 主导出组件：包装 RestartProvider
export function BotConfigPage() {
  return (
    <RestartProvider>
      <BotConfigPageContent />
    </RestartProvider>
  )
}

// 内部实现组件
function BotConfigPageContent() {
  const navigate = useNavigate()
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [autoSaving, setAutoSaving] = useState(false)
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false)
  const [editMode, setEditMode] = useState<BotSettingsMode>('detail')
  const [sourceCode, setSourceCode] = useState<string>('')
  const [hasTomlError, setHasTomlError] = useState(false)
  const [tomlErrorMessage, setTomlErrorMessage] = useState<string>('')
  const [showFileModeNotice, setShowFileModeNotice] = useState(
    () => localStorage.getItem(FILE_MODE_NOTICE_DISMISSED_KEY) !== 'true'
  )
  const { toast } = useToast()
  const { triggerRestart, isRestarting } = useRestart()
  const routeSearch = useRouterState({ select: (state) => state.location.searchStr })
  const searchFieldPath = useMemo(() => getConfigSearchField(routeSearch), [routeSearch])
  const lastRouteSearchRef = useRef<string | null>(null)

  // 同页链接可定位共享组或 WebUI 设置；未保存的配置完成后再切换。
  useEffect(() => {
    if (lastRouteSearchRef.current === routeSearch) return
    const mode = new URLSearchParams(routeSearch).get('mode')
    if (hasUnsavedChanges) return
    lastRouteSearchRef.current = routeSearch
    if (mode === 'groups' || mode === 'webui' || mode === 'commands') {
      setEditMode(mode)
    } else {
      setEditMode('detail')
    }
  }, [hasUnsavedChanges, routeSearch])

  const [sectionValues, setSectionValues] = useState<Record<string, ConfigSectionData | null>>({})

  // Schema 状态（用于动态 tab 分组）
  const [configSchema, setConfigSchema] = useState<ConfigSchema | null>(null)

  // 用于标记初始加载和配置缓存
  const initialLoadRef = useRef(true)
  const configRef = useRef<Record<string, unknown>>({})

  // ==================== 辅助函数 ====================

  /**
   * 翻译 TOML 错误信息为中文
   */
  const translateTomlError = (errorMessage: string): string => {
    // 分行处理，保留多行格式
    const lines = errorMessage.split('\n')

    // 翻译第一行（主要错误信息）
    let firstLine = lines[0]

    // 移除 "Error: " 前缀（如果有）
    firstLine = firstLine.replace(/^Error:\s*/, '')

    // 常见 TOML 错误模式匹配和翻译
    const translations: Array<[RegExp, string | ((match: RegExpMatchArray) => string)]> = [
      // Invalid TOML document 系列
      [
        /Invalid TOML document: unrecognized escape sequence/,
        'TOML 文档错误：无法识别的转义序列（提示：在双引号字符串中使用 \\\\ 转义反斜杠，或使用单引号字符串）',
      ],
      [
        /Invalid TOML document: (?:only letter, numbers, dashes and underscores are allowed in keys|illegal character in key)/,
        'TOML 文档错误：键名只能包含字母、数字、短横线和下划线',
      ],
      [/Invalid TOML document: (.+)/, 'TOML 文档错误：$1'],

      // 位置错误系列
      [/Unexpected character.*at line (\d+), column (\d+)/, '第 $1 行第 $2 列：意外的字符'],
      [/Expected.*at line (\d+), column (\d+)/, '第 $1 行第 $2 列：缺少必要的字符'],
      [/Invalid.*at line (\d+), column (\d+)/, '第 $1 行第 $2 列：无效的语法'],
      [/Unterminated string at line (\d+)/, '第 $1 行：字符串未正常结束（缺少引号）'],
      [/Duplicate key.*at line (\d+)/, '第 $1 行：重复的键名'],
      [
        /Invalid escape sequence at line (\d+)/,
        '第 $1 行：无效的转义序列（提示：在双引号字符串中使用 \\\\ 转义反斜杠）',
      ],
      [/Expected.*but got.*at line (\d+)/, '第 $1 行：类型不匹配'],
      [/line (\d+), column (\d+)/, '第 $1 行第 $2 列'],

      // 通用错误系列
      [/Unexpected end of input/, '意外的文件结束（可能缺少闭合符号）'],
      [/Unexpected token/, '意外的标记'],
      [/Invalid number/, '无效的数字'],
      [/Invalid date/, '无效的日期格式'],
      [/Invalid boolean/, '无效的布尔值（应为 true 或 false）'],
      [/Unexpected character/, '意外的字符'],
      [/unrecognized escape sequence/, '无法识别的转义序列'],
    ]

    // 尝试翻译第一行
    for (const [pattern, replacement] of translations) {
      if (pattern.test(firstLine)) {
        firstLine = firstLine.replace(pattern, replacement as string)
        break
      }
    }

    // 重组多行错误信息
    if (lines.length > 1) {
      lines[0] = firstLine
      return lines.join('\n')
    }

    return firstLine
  }

  /**
   * 解析并设置所有配置状态
   * 抽取自 loadConfig 和 handleModeChange 中的重复逻辑
   */
  const parseAndSetConfig = useCallback((config: Record<string, unknown>) => {
    const { memory: _legacyMemory, ...configWithoutLegacyMemory } = config
    configRef.current = configWithoutLegacyMemory

    setSectionValues(
      Object.fromEntries(
        Object.entries(configWithoutLegacyMemory).map(([sectionName, sectionValue]) => [
          sectionName,
          (sectionValue ?? {}) as ConfigSectionData,
        ])
      )
    )
  }, [])

  /**
   * 构建完整的配置对象用于保存
   */
  const buildFullConfig = useCallback(() => {
    const cleanSectionValues = Object.fromEntries(
      Object.entries(sectionValues).filter(([, value]) => value !== null)
    )

    return {
      ...configRef.current,
      ...cleanSectionValues,
    }
  }, [sectionValues])

  // 加载源代码
  const loadSourceCode = useCallback(async () => {
    try {
      const result = await getBotConfigRaw()
      const raw = (result as unknown as Record<string, unknown>).content as string
      // 将 TOML 基本字符串中的转义序列转换为实际字符以便在编辑器中正确显示
      // 使用正则表达式只处理双引号字符串内的转义序列，不影响单引号字符串
      const unescaped = raw.replace(/"([^"]*)"/g, (_match, content) => {
        const decoded = content
          .replace(/\\n/g, '\n') // 换行符
          .replace(/\\t/g, '\t') // 制表符
          .replace(/\\r/g, '\r') // 回车符
          .replace(/\\"/g, '"') // 双引号
          .replace(/\\\\/g, '\\') // 反斜杠（必须放在最后）
        return `"${decoded}"`
      })
      setSourceCode(unescaped)
      setHasTomlError(false)
    } catch (error) {
      toast({
        variant: 'destructive',
        title: '加载失败',
        description: error instanceof Error ? error.message : '加载源代码失败',
      })
    }
  }, [toast])

  // 加载配置
  const loadConfig = useCallback(async (): Promise<boolean> => {
    try {
      setLoading(true)
      // 用 allSettled：主配置为必需，schema 为可选，二者失败互不影响
      const [result, schemaResult] = await Promise.allSettled([
        getBotConfigCached(),
        getBotConfigSchema(),
      ])
      if (result.status !== 'fulfilled') {
        toast({
          title: '加载失败',
          description: result.reason instanceof Error ? result.reason.message : '加载配置失败',
          variant: 'destructive',
        })
        setLoading(false)
        return false
      }
      parseAndSetConfig(result.value)
      if (schemaResult.status === 'fulfilled' && schemaResult.value) {
        setConfigSchema(
          (schemaResult.value as unknown as Record<string, unknown>).schema as ConfigSchema
        )
      }
      setHasUnsavedChanges(false)
      initialLoadRef.current = false
      return true
    } catch (error) {
      console.error('加载配置失败:', error)
      toast({
        title: '加载失败',
        description: '无法加载配置文件',
        variant: 'destructive',
      })
      return false
    } finally {
      setLoading(false)
    }
  }, [toast, parseAndSetConfig])

  useEffect(() => {
    void loadConfig()
  }, [loadConfig])

  useEffect(() => {
    const hookEntries = [
      ['bot.platform', HiddenFieldHook, 'hidden'],
      ['bot.alias_names', AliasNamesHook],
      ['bot.qq_account', HiddenFieldHook, 'hidden'],
      ['bot.platforms', HiddenFieldHook, 'hidden'],
      ['personality.multiple_reply_style', MultipleReplyStyleHook],
      ['chat.reply_style.chat_prompts', ChatPromptsHook],
      ['chat.reply_timing.talk_value_rules', ChatTalkValueRulesHook],
      ['experimental.focus_chat_whitelist', FocusWhitelistHook],
      ['experimental.focus_groups', BehaviorFocusGroupsHook],
      ['experimental.behavior_groups', BehaviorGroupsHook],
      ['experimental.behavior_learning_list', BehaviorLearningListHook],
      ['expression.expression_groups', ExpressionGroupsHook],
      ['expression.learning_list', ExpressionLearningListHook],
      ['jargon.jargon_groups', JargonGroupsHook],
      ['jargon.learning_list', JargonLearningListHook],
      ['a_memorix.global_memory_sharing_enabled', HiddenFieldHook, 'hidden'],
      ['a_memorix.shared_memory_groups', AMemorixSharedMemoryGroupsHook],
      ['a_memorix.filter.chats', AMemorixRetrievalChatsHook],
      ['a_memorix.filter.retrieval', AMemorixRetrievalFilterGroupHook, 'wrapper'],
      ['a_memorix.filter.retrieval.chat_stream.chats', AMemorixRetrievalChatsHook],
      ['a_memorix.filter.retrieval.chat_summary.chats', AMemorixRetrievalChatsHook],
      ['a_memorix.filter.retrieval.episode.chats', AMemorixRetrievalChatsHook],
      ['keyword_reaction.keyword_rules', KeywordRulesHook],
      ['keyword_reaction.regex_rules', RegexRulesHook],
      ['mcp.client.roots.items', MCPRootItemsHook],
      ['mcp.servers', MCPServersHook],
    ] as const

    for (const [fieldPath, hookComponent, hookType = 'replace'] of hookEntries) {
      fieldHooks.register(fieldPath, hookComponent, hookType)
    }

    return () => {
      for (const [fieldPath] of hookEntries) {
        fieldHooks.unregister(fieldPath)
      }
    }
  })

  const { triggerAutoSave, cancelPendingAutoSave, resetAutoSaveState, runWithAutoSaveBarrier } =
    useAutoSave(initialLoadRef.current, setAutoSaving, setHasUnsavedChanges)

  const dismissFileModeNotice = useCallback(() => {
    localStorage.setItem(FILE_MODE_NOTICE_DISMISSED_KEY, 'true')
    setShowFileModeNotice(false)
  }, [])

  // 保存源代码
  const saveSourceCode = async () => {
    try {
      setSaving(true)
      // 编辑器展示时会把 basic string 内的 \n 展开成真实换行；保存前先转回 TOML 转义序列。
      const escapedSourceCode = sourceCode.replace(/"([^"]*)"/g, (_match, content) => {
        const encoded = content
          .replace(/\\/g, '\\\\') // 反斜杠必须先转义，避免 \s 等序列被 TOML 当作非法转义
          .replace(/"/g, '\\"')
          .replace(/\n/g, '\\n')
          .replace(/\t/g, '\\t')
          .replace(/\r/g, '\\r')
        return `"${encoded}"`
      })

      // 前端验证 TOML 格式
      try {
        parseToml(escapedSourceCode)
      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : 'TOML 格式错误'
        const translatedMsg = translateTomlError(errorMsg)
        setHasTomlError(true)
        setTomlErrorMessage(translatedMsg)
        toast({
          variant: 'destructive',
          title: 'TOML 格式错误',
          description: translatedMsg,
        })
        setSaving(false)
        return
      }

      await updateBotConfigRaw(escapedSourceCode)
      setHasUnsavedChanges(false)
      setHasTomlError(false)
      setTomlErrorMessage('')
      toast({
        title: '保存成功',
        description: '配置已保存',
      })
      // 重新加载可视化配置
      if (await loadConfig()) {
        resetAutoSaveState()
      }
    } catch (error) {
      setHasTomlError(true)
      const errorMsg = error instanceof Error ? error.message : '保存配置失败'
      setTomlErrorMessage(errorMsg)
      toast({
        variant: 'destructive',
        title: '保存失败',
        description: errorMsg,
      })
    } finally {
      setSaving(false)
    }
  }

  // 处理模式切换
  const handleModeChange = async (mode: BotSettingsMode) => {
    if (hasUnsavedChanges) {
      toast({
        variant: 'destructive',
        title: '切换失败',
        description: '请先保存当前更改',
      })
      return
    }

    setEditMode(mode)
    const params = new URLSearchParams(routeSearch)
    params.delete('field')
    params.delete('tab')
    if (mode === 'detail' || mode === 'source') {
      params.delete('mode')
    } else {
      params.set('mode', mode)
    }
    const nextSearch = params.size ? `?${params.toString()}` : ''
    lastRouteSearchRef.current = nextSearch
    await navigate({ href: `/config/bot${nextSearch}`, replace: true })
    if (mode === 'webui') return
    if (mode === 'source') {
      await loadSourceCode()
    } else {
      // 切换回可视化时,直接重新加载配置但不显示全局 loading
      try {
        const result = await getBotConfig()
        parseAndSetConfig(result)
        resetAutoSaveState()
        setHasUnsavedChanges(false)
      } catch (error) {
        console.error('加载配置失败:', error)
        toast({
          title: '加载失败',
          description: '无法加载配置文件',
          variant: 'destructive',
        })
      }
    }
  }

  // 手动保存
  const saveConfig = async () => {
    try {
      setSaving(true)
      const configToSave = buildFullConfig()
      await runWithAutoSaveBarrier(() => updateBotConfig(configToSave))
      toast({
        title: '保存成功',
        description: '麦麦设置已保存',
      })
    } catch (error) {
      console.error('保存配置失败:', error)
      toast({
        title: '保存失败',
        description: (error as Error).message,
        variant: 'destructive',
      })
    } finally {
      setSaving(false)
    }
  }

  const handleReloadFromFile = async () => {
    await cancelPendingAutoSave()
    const loaded = await loadConfig()
    if (!loaded) {
      return
    }
    resetAutoSaveState()
    if (editMode === 'source') {
      await loadSourceCode()
    }
    toast({
      title: '已刷新',
      description: '已从 bot_config.toml 重新读取配置',
    })
  }

  // 重启麦麦
  const handleRestart = async () => {
    await triggerRestart()
  }

  // 保存并重启
  const handleSaveAndRestart = async () => {
    try {
      setSaving(true)
      const configToSave = buildFullConfig()
      await runWithAutoSaveBarrier(() => updateBotConfig(configToSave))
      toast({
        title: '保存成功',
        description: '配置已保存，即将重启麦麦...',
      })
      // 等待一下让用户看到保存成功的提示
      await new Promise((resolve) => setTimeout(resolve, TOAST_DISPLAY_DELAY))
      await handleRestart()
    } catch (error) {
      console.error('保存失败:', error)
      toast({
        title: '保存失败',
        description: (error as Error).message,
        variant: 'destructive',
      })
    } finally {
      setSaving(false)
    }
  }
  // 保留给后续恢复重启入口或快捷操作复用；当前页面不渲染重启按钮。
  void handleSaveAndRestart

  // 根据 schema 构建 tab 分组
  const tabGroups = useMemo(() => {
    if (!configSchema) return []
    return buildTabGroupsFromSchema(configSchema)
  }, [configSchema])

  useEffect(() => {
    if (!searchFieldPath) {
      return
    }

    const frameId = window.requestAnimationFrame(() => setEditMode('detail'))
    return () => window.cancelAnimationFrame(frameId)
  }, [searchFieldPath])

  const setSectionValue = useCallback(
    (sectionName: string, value: ConfigSectionData) => {
      setSectionValues((current) => ({
        ...current,
        [sectionName]: value,
      }))
      triggerAutoSave(sectionName, value)
    },
    [triggerAutoSave]
  )

  if (loading) {
    return (
      <ScrollArea className="h-full">
        <div className="space-y-4 p-4 sm:space-y-6 sm:p-6">
          <div className="flex h-64 items-center justify-center">
            <ThinkingIllustration size="lg" />
          </div>
        </div>
      </ScrollArea>
    )
  }

  return (
    <ScrollArea className="h-full min-w-0" scrollbars="vertical">
      <div className="max-w-full space-y-4 overflow-x-hidden p-4 sm:space-y-6 sm:p-6">
        {/* 页面标题 */}
        <div className="flex flex-col gap-3 sm:gap-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="min-w-0">
              <h1 className="text-xl font-bold sm:text-2xl md:text-3xl">麦麦设置</h1>
            </div>
            {/* 按钮组 - 桌面端靠右 */}
            <div className="flex w-full min-w-0 flex-wrap gap-2 sm:w-auto sm:flex-shrink-0 sm:justify-end">
              <Tabs
                value={editMode}
                onValueChange={(v) => handleModeChange(v as BotSettingsMode)}
                className="w-full min-w-0 sm:w-96"
              >
                <TabsList data-config-bot-mode-tabs="true" className="grid h-9 w-full grid-cols-3">
                  <TabsTrigger value="detail" className="px-2 text-sm">
                    <SlidersHorizontal className="mr-1 h-4 w-4" />
                    详细设置
                  </TabsTrigger>
                  <TabsTrigger value="commands" className="px-2 text-sm">
                    <ShieldCheck className="mr-1 h-4 w-4" />
                    命令管理
                  </TabsTrigger>
                  <TabsTrigger value="webui" className="px-2 text-sm">
                    <Settings className="mr-1 h-4 w-4" />
                    WebUI 设置
                  </TabsTrigger>
                </TabsList>
              </Tabs>
              {editMode !== 'webui' && (
                <>
                  <Button
                    onClick={handleReloadFromFile}
                    disabled={saving || autoSaving || isRestarting}
                    size="sm"
                    variant="outline"
                    className="h-9 w-9 flex-none px-0"
                    aria-label="刷新"
                    title="刷新"
                  >
                    <RefreshCw className="h-4 w-4" />
                  </Button>
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        className="h-9 w-9 flex-none px-0"
                        aria-label="更多设置"
                        title="更多设置"
                      >
                        <MoreHorizontal className="h-4 w-4" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end" className="w-44">
                      <DropdownMenuItem
                        disabled={saving || autoSaving || !hasUnsavedChanges || isRestarting}
                        onSelect={() => void (editMode === 'source' ? saveSourceCode() : saveConfig())}
                      >
                        <Save className="mr-2 h-4 w-4" />
                        手动保存
                        <span className="text-muted-foreground ml-auto text-xs">
                          {saving
                            ? '保存中'
                            : autoSaving
                              ? '自动保存中'
                              : !hasUnsavedChanges
                                ? '已保存'
                                : ''}
                        </span>
                      </DropdownMenuItem>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem onSelect={() => void handleModeChange('groups')}>
                        <UsersRound className="mr-2 h-4 w-4" />
                        共享组设置
                        {editMode === 'groups' && <Check className="ml-auto h-4 w-4" />}
                      </DropdownMenuItem>
                      <DropdownMenuItem onSelect={() => void handleModeChange('source')}>
                        <Code2 className="mr-2 h-4 w-4" />
                        源文件编辑
                        {editMode === 'source' && <Check className="ml-auto h-4 w-4" />}
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </>
              )}
            </div>
          </div>
        </div>

        {editMode === 'webui' && (
          <Suspense fallback={<ThinkingIllustration size="lg" />}>
            <WebUISettings />
          </Suspense>
        )}

        {/* 源代码模式 */}
        {editMode === 'source' && (
          <div className="space-y-4">
            {(showFileModeNotice || (hasTomlError && tomlErrorMessage)) && (
              <Alert className={showFileModeNotice ? 'pr-10' : undefined}>
                <Info className="h-4 w-4" />
                {showFileModeNotice && (
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="absolute top-2 right-2 h-6 w-6 px-0"
                    aria-label="关闭文件模式提示"
                    title="关闭文件模式提示"
                    onClick={dismissFileModeNotice}
                  >
                    <X className="h-3.5 w-3.5" />
                  </Button>
                )}
                <AlertDescription>
                  {showFileModeNotice && (
                    <>
                      <strong>文件模式：</strong>直接编辑原始配置文件。此功能仅适用于熟悉 TOML
                      语法的用户。只有格式完全正确才能保存。
                    </>
                  )}
                  {hasTomlError && tomlErrorMessage && (
                    <div className="text-destructive bg-destructive/10 mt-3 rounded-md p-3 font-semibold">
                      <div className="mb-2 font-bold">⚠️ TOML 格式错误：</div>
                      <pre className="font-mono text-sm break-words whitespace-pre-wrap">
                        {tomlErrorMessage}
                      </pre>
                    </div>
                  )}
                </AlertDescription>
              </Alert>
            )}

            <CodeEditor
              value={sourceCode}
              onChange={(value) => {
                setSourceCode(value)
                setHasUnsavedChanges(true)
                // 清除之前的错误状态
                if (hasTomlError) {
                  setHasTomlError(false)
                  setTomlErrorMessage('')
                }
              }}
              language="toml"
              height="calc(100vh - 280px)"
              minHeight="500px"
              placeholder="TOML 配置内容"
            />
          </div>
        )}

        {editMode === 'groups' && (
          <SharedGroupsSettings
            onSectionSaved={(sectionName, value) => {
              configRef.current = { ...configRef.current, [sectionName]: value }
              setSectionValues((current) => ({ ...current, [sectionName]: value }))
            }}
          />
        )}

        {/* 详细设置模式（原可视化模式） */}
        {editMode === 'detail' && (
          <DynamicConfigTabs
            configSchema={configSchema}
            tabGroups={tabGroups}
            sectionValues={sectionValues}
            setSectionValue={setSectionValue}
            setHasUnsavedChanges={setHasUnsavedChanges}
            searchFieldPath={searchFieldPath}
          />
        )}

        {editMode === 'commands' && (
          <CommandPermissions
            pluginSection={sectionValues.plugin ?? null}
            onChange={(value) => {
              setSectionValue('plugin', value)
              setHasUnsavedChanges(true)
            }}
          />
        )}

        {/* 重启遮罩层 */}
        <RestartOverlay />
      </div>
    </ScrollArea>
  )
}

// ==================== 动态 Tab 渲染组件 ====================

function updateNestedValue(
  target: ConfigSectionData | null | undefined,
  pathSegments: string[],
  value: unknown
): ConfigSectionData {
  const currentTarget = target && typeof target === 'object' && !Array.isArray(target) ? target : {}
  const [currentPath, ...restPath] = pathSegments

  if (!currentPath) {
    return currentTarget
  }

  if (restPath.length === 0) {
    return {
      ...currentTarget,
      [currentPath]: value,
    }
  }

  return {
    ...currentTarget,
    [currentPath]: updateNestedValue(
      currentTarget[currentPath] as ConfigSectionData | undefined,
      restPath,
      value
    ),
  }
}

interface DynamicConfigTabsProps {
  configSchema: ConfigSchema | null
  tabGroups: TabGroup[]
  sectionValues: Record<string, ConfigSectionData | null>
  setSectionValue: (sectionName: string, value: ConfigSectionData) => void
  setHasUnsavedChanges: (v: boolean) => void
  searchFieldPath: string
}

function ConfigTabsExpandButton({ expanded, onClick }: { expanded: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      className="inline-flex h-9 w-9 shrink-0 cursor-pointer items-center justify-center self-center border-0 bg-transparent p-0 text-foreground transition-colors hover:text-primary focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
      aria-label={expanded ? '收起设置栏目' : '展开更多设置栏目'}
      aria-expanded={expanded}
      title={expanded ? '收起设置栏目' : '展开更多设置栏目'}
      onClick={onClick}
    >
      <ChevronRight
        aria-hidden="true"
        strokeWidth={3.5}
        className={cn('h-6 w-6 motion-safe:transition-transform motion-safe:duration-300', expanded && 'rotate-180')}
      />
    </button>
  )
}

function DynamicConfigTabs(props: DynamicConfigTabsProps) {
  const {
    configSchema,
    searchFieldPath,
    sectionValues,
    setHasUnsavedChanges,
    setSectionValue,
    tabGroups,
  } = props
  const initialActiveTab = tabGroups[0]?.id ?? ''
  const [expanded, setExpanded] = useState(false)
  const [activeTab, setActiveTab] = useState(initialActiveTab)
  const [expandedSubtabGroups, setExpandedSubtabGroups] = useState<Record<string, boolean>>({})
  const [activeSubtabByGroup, setActiveSubtabByGroup] = useState<Record<string, string>>({})
  const [advancedVisible, setAdvancedVisible] = useState(false)
  const [tabGuideVisible, setTabGuideVisible] = useState(
    () => localStorage.getItem('bot-config-tabs-guide-dismissed') !== 'true'
  )
  const [experimentalNoticeOpen, setExperimentalNoticeOpen] = useState(
    () =>
      initialActiveTab === 'experimental' &&
      localStorage.getItem(EXPERIMENTAL_FEATURES_NOTICE_DISMISSED_KEY) !== 'true'
  )
  const scrolledSearchFieldRef = useRef('')

  if (!tabGroups.some((tab) => tab.id === activeTab)) {
    const fallbackTab = tabGroups[0]?.id ?? ''
    if (activeTab !== fallbackTab) {
      setActiveTab(fallbackTab)
    }
  }

  useEffect(() => {
    if (!searchFieldPath) {
      return
    }

    const [sectionName, subcategoryName] = searchFieldPath.split('.')
    const targetTab = tabGroups.find((tab) => tab.sections.includes(sectionName))
    if (!targetTab) {
      return
    }

    const frameId = window.requestAnimationFrame(() => {
      setActiveTab(targetTab.id)
      setAdvancedVisible(true)
      if (targetTab.advanced) {
        setExpanded(true)
      }

      if (subcategoryName) {
        const subtabId = `${sectionName}.${subcategoryName}`
        setActiveSubtabByGroup((current) => ({
          ...current,
          [targetTab.id]: subtabId,
        }))
        setExpandedSubtabGroups((current) => ({
          ...current,
          [targetTab.id]: true,
        }))
      }
    })

    return () => window.cancelAnimationFrame(frameId)
  }, [searchFieldPath, tabGroups])

  useEffect(() => {
    if (!searchFieldPath || scrolledSearchFieldRef.current === searchFieldPath) {
      return
    }

    let nestedFrameId = 0
    const frameId = window.requestAnimationFrame(() => {
      nestedFrameId = window.requestAnimationFrame(() => {
        if (scrollToConfigSearchField(searchFieldPath)) {
          scrolledSearchFieldRef.current = searchFieldPath
        }
      })
    })

    return () => {
      window.cancelAnimationFrame(frameId)
      if (nestedFrameId) {
        window.cancelAnimationFrame(nestedFrameId)
      }
    }
  }, [activeSubtabByGroup, activeTab, advancedVisible, expanded, searchFieldPath])

  if (tabGroups.length === 0 || !configSchema?.nested) {
    return null
  }

  const defaultTabGroups = tabGroups.filter((tab) => !tab.advanced)
  const expandedTabGroups = tabGroups.filter((tab) => tab.advanced)
  const visibleTabGroups = expanded ? [...defaultTabGroups, ...expandedTabGroups] : defaultTabGroups
  const hasCollapsibleTabs = tabGroups.some((tab) => tab.advanced)

  const toggleExpanded = () => {
    setExpanded((current) => {
      if (current && tabGroups.find((tab) => tab.id === activeTab)?.advanced) {
        const firstDefaultTab = tabGroups.find((tab) => !tab.advanced)
        setActiveTab(firstDefaultTab?.id ?? tabGroups[0]?.id ?? '')
      }
      return !current
    })
  }

  const dismissTabGuide = () => {
    localStorage.setItem('bot-config-tabs-guide-dismissed', 'true')
    setTabGuideVisible(false)
  }

  const dismissExperimentalNotice = () => {
    localStorage.setItem(EXPERIMENTAL_FEATURES_NOTICE_DISMISSED_KEY, 'true')
    setExperimentalNoticeOpen(false)
  }

  const handleTabChange = (value: string) => {
    setActiveTab(value)
    if (
      value === 'experimental' &&
      localStorage.getItem(EXPERIMENTAL_FEATURES_NOTICE_DISMISSED_KEY) !== 'true'
    ) {
      setExperimentalNoticeOpen(true)
    }
  }

  const updateSectionValueByPath = (sectionName: string, restPath: string[], value: unknown) => {
    const currentSectionValue = sectionValues[sectionName] ?? {}
    const nextSectionValue =
      restPath.length === 0
        ? (value as ConfigSectionData)
        : updateNestedValue(currentSectionValue, restPath, value)

    setSectionValue(sectionName, nextSectionValue)
    setHasUnsavedChanges(true)
  }

  const identityFieldNames = new Set(['nickname', 'alias_names'])
  const basicChatPromptFieldNames = new Set(['group_chat_prompt', 'private_chat_prompts'])
  const botSchema = configSchema.nested.bot
  const replyStyleSchema = configSchema.nested.chat?.nested?.reply_style
  const hasPersonalitySchema = Boolean(configSchema.nested.personality)
  const identitySchema: ConfigSchema | null = botSchema ? {
    ...botSchema,
    fields: botSchema.fields
      .filter((field) => identityFieldNames.has(field.name))
      .map((field) => ({ ...field, advanced: false, 'x-row': 'bot-identity' })),
    nested: {},
  } : null
  const basicChatPromptSchema: ConfigSchema | null = replyStyleSchema ? {
    ...replyStyleSchema,
    fields: replyStyleSchema.fields
      .filter((field) => basicChatPromptFieldNames.has(field.name))
      .map((field) => ({ ...field, advanced: false })),
    nested: {},
  } : null
  const sectionLeadingContent = identitySchema ? {
    personality: (
      <div className="space-y-3">
        {identitySchema && (
          <DynamicConfigForm
            schema={identitySchema}
            values={sectionValues.bot ?? {}}
            onChange={(field, value) => updateSectionValueByPath('bot', [field], value)}
            basePath="bot"
            hooks={fieldHooks}
            advancedVisible={advancedVisible}
          />
        )}
      </div>
    ),
  } : undefined
  const sectionTrailingContent = {
    personality: (
      <GlobalLearningSettings
        values={sectionValues}
        sections={(['expression', 'jargon'] as const).filter((section) => configSchema.nested?.[section])}
        onChange={(section, rules) => updateSectionValueByPath(section, ['learning_list'], rules)}
      />
    ),
  }
  const fieldTrailingContent = basicChatPromptSchema ? {
    'personality.reply_style': (
      <DynamicConfigForm
        schema={basicChatPromptSchema}
        values={(sectionValues.chat?.reply_style as ConfigSectionData) ?? {}}
        onChange={(field, value) => updateSectionValueByPath('chat', ['reply_style', field], value)}
        basePath="chat.reply_style"
        hooks={fieldHooks}
        advancedVisible={advancedVisible}
      />
    ),
  } : undefined

  // 仅调整展示归属，身份信息与聊天提示词仍通过原配置路径读写。
  const getDisplaySectionSchema = (sectionName: string, schema: ConfigSchema): ConfigSchema => {
    if (sectionName === 'bot' && configSchema.nested?.personality) {
      return { ...schema, fields: schema.fields.filter((field) => !identityFieldNames.has(field.name)) }
    }
    if (sectionName === 'personality') {
      return { ...schema, uiLabel: '身份与人格' }
    }
    if (sectionName === 'chat' && replyStyleSchema) {
      return {
        ...schema,
        nested: {
          ...schema.nested,
          reply_style: {
            ...replyStyleSchema,
            uiLabel: '聊天流prompt',
            uiSubLabel: '聊天流prompt',
            uiAdvanced: true,
            fields: hasPersonalitySchema
              ? replyStyleSchema.fields.filter((field) => !basicChatPromptFieldNames.has(field.name))
              : replyStyleSchema.fields,
          },
        },
      }
    }
    return schema
  }

  const getSubtabLabel = (schema: ConfigSchema, fallback: string) => {
    return schema.uiSubLabel || schema.uiLabel || schema.classDoc || fallback
  }

  const getObjectSubcategoryEntries = (sectionSchema: ConfigSchema) => {
    const sectionFieldByName = new Map(sectionSchema.fields.map((field) => [field.name, field]))
    return Object.entries(sectionSchema.nested ?? {}).filter(([subcategoryName]) => {
      return sectionFieldByName.get(subcategoryName)?.type === 'object'
    })
  }

  const renderSubtabbedContent = (
    tabId: string,
    tabNestedEntries: readonly (readonly [string, ConfigSchema])[]
  ) => {
    const subtabPanes: SubtabPane[] = []
    const entryMap = new Map<string, ConfigSchema>(
      tabNestedEntries.map(([sectionName, schema]) => [sectionName, schema])
    )
    const childSectionsByParent = new Map<string, Array<readonly [string, ConfigSchema]>>()

    for (const [sectionName, sectionSchema] of tabNestedEntries) {
      const parentName = sectionSchema.uiParent
      if (!parentName || !entryMap.has(parentName)) {
        continue
      }

      const childSections = childSectionsByParent.get(parentName) ?? []
      childSections.push([sectionName, sectionSchema])
      childSectionsByParent.set(parentName, childSections)
    }

    const collectDescendantEntries = (
      parentName: string
    ): Array<readonly [string, ConfigSchema]> => {
      const directChildEntries = childSectionsByParent.get(parentName) ?? []

      return directChildEntries.flatMap(([childName, childSchema]) => {
        return [[childName, childSchema] as const, ...collectDescendantEntries(childName)]
      })
    }

    const renderSectionGroupContent = (sectionEntries: Array<readonly [string, ConfigSchema]>) => {
      const values = Object.fromEntries(
        sectionEntries.map(([sectionName]) => [sectionName, sectionValues[sectionName] ?? {}])
      )
      const groupSchema: ConfigSchema = {
        className: sectionEntries.map(([sectionName]) => sectionName).join('.'),
        classDoc: sectionEntries[0]?.[1].uiLabel || sectionEntries[0]?.[1].classDoc || '',
        fields: [],
        nested: Object.fromEntries(sectionEntries),
      }

      return (
        <DynamicConfigForm
          schema={groupSchema}
          values={values}
          onChange={(fieldPath, value) => {
            const [sectionName, ...restPath] = fieldPath.split('.')
            if (!sectionName) {
              return
            }

            updateSectionValueByPath(sectionName, restPath, value)
          }}
          hooks={fieldHooks}
          advancedVisible={advancedVisible}
          sectionColumns={2}
          sectionLeadingContent={sectionLeadingContent}
          sectionTrailingContent={sectionTrailingContent}
          fieldTrailingContent={fieldTrailingContent}
        />
      )
    }

    for (const [sectionName, sectionSchema] of tabNestedEntries) {
      if (sectionSchema.uiParent && entryMap.has(sectionSchema.uiParent)) {
        continue
      }

      const sectionValue = (sectionValues[sectionName] ?? {}) as ConfigSectionData
      const allSubcategoryEntries = sectionSchema.uiUseSubTabs
        ? getObjectSubcategoryEntries(sectionSchema)
        : []
      const subcategoryEntries = allSubcategoryEntries
      const subcategoryNames = new Set(
        allSubcategoryEntries.map(([subcategoryName]) => subcategoryName)
      )
      const rootFields = sectionSchema.fields.filter((field) => !subcategoryNames.has(field.name))
      const rootSchema: ConfigSchema = {
        ...sectionSchema,
        className: `${sectionSchema.className}Root`,
        fields: rootFields,
        nested: {},
      }

      if (rootFields.length > 0) {
        subtabPanes.push({
          advanced: Boolean(sectionSchema.uiAdvanced),
          id: sectionName,
          label: sectionSchema.uiRootSubLabel || getSubtabLabel(sectionSchema, sectionName),
          content: (
            <DynamicConfigForm
              schema={rootSchema}
              values={sectionValue}
              onChange={(fieldPath, value) =>
                updateSectionValueByPath(sectionName, fieldPath.split('.'), value)
              }
              basePath={sectionName}
              hooks={fieldHooks}
              advancedVisible={advancedVisible}
              sectionColumns={1}
            />
          ),
        })
      }

      for (const [subcategoryName, subcategorySchema] of subcategoryEntries) {
        subtabPanes.push({
          advanced: Boolean(subcategorySchema.uiAdvanced),
          id: `${sectionName}.${subcategoryName}`,
          label: getSubtabLabel(subcategorySchema, subcategoryName),
          content: (
            <DynamicConfigForm
              schema={subcategorySchema}
              values={(sectionValue[subcategoryName] as Record<string, unknown>) || {}}
              onChange={(fieldPath, value) =>
                updateSectionValueByPath(
                  sectionName,
                  [subcategoryName, ...fieldPath.split('.')],
                  value
                )
              }
              basePath={`${sectionName}.${subcategoryName}`}
              hooks={fieldHooks}
              advancedVisible={advancedVisible}
              sectionColumns={1}
            />
          ),
        })
      }

      for (const [childName, childSchema] of childSectionsByParent.get(sectionName) ?? []) {
        const sectionGroupEntries = [
          [childName, childSchema] as const,
          ...collectDescendantEntries(childName),
        ]
        subtabPanes.push({
          advanced: Boolean(childSchema.uiAdvanced),
          id: childName,
          label: getSubtabLabel(childSchema, childName),
          content: renderSectionGroupContent(sectionGroupEntries),
        })
      }
    }

    if (subtabPanes.length === 0) {
      return null
    }

    const subtabExpanded = Boolean(expandedSubtabGroups[tabId])
    const defaultSubtabPanes = subtabPanes.filter((pane) => !pane.advanced)
    const expandedSubtabPanes = subtabPanes.filter((pane) => pane.advanced)
    const visibleSubtabPanes = subtabExpanded
      ? [...defaultSubtabPanes, ...expandedSubtabPanes]
      : defaultSubtabPanes
    const hasCollapsibleSubtabs = subtabPanes.some((pane) => pane.advanced)
    const visibleSubtabIds = new Set(visibleSubtabPanes.map((pane) => pane.id))
    const activeSubtab = activeSubtabByGroup[tabId]
    const resolvedActiveSubtab = visibleSubtabIds.has(activeSubtab)
      ? activeSubtab
      : (visibleSubtabPanes[0]?.id ?? subtabPanes[0].id)

    const toggleSubtabsExpanded = () => {
      if (
        subtabExpanded &&
        subtabPanes.find((pane) => pane.id === resolvedActiveSubtab)?.advanced
      ) {
        setActiveSubtabByGroup((current) => ({
          ...current,
          [tabId]: defaultSubtabPanes[0]?.id ?? subtabPanes[0].id,
        }))
      }

      setExpandedSubtabGroups((current) => ({
        ...current,
        [tabId]: !subtabExpanded,
      }))
    }

    return (
      <Tabs
        key={subtabPanes.map((pane) => pane.id).join('|')}
        value={resolvedActiveSubtab}
        onValueChange={(value) =>
          setActiveSubtabByGroup((current) => ({
            ...current,
            [tabId]: value,
          }))
        }
        className="space-y-3"
      >
        <DashboardTabBar
          data-config-bot-subtab-list="true"
          variant="scroll"
          className="bg-background/80 h-11 border"
        >
          {visibleSubtabPanes.map((pane) => (
            <Fragment key={pane.id}>
              <DashboardTabTrigger
                value={pane.id}
                data-config-bot-extra-tab={pane.advanced ? 'true' : undefined}
                className={cn(
                  'min-h-8 text-base font-semibold',
                  pane.advanced &&
                    'overflow-hidden text-muted-foreground/80 hover:bg-background/70 data-[state=active]:bg-primary/10 data-[state=active]:text-primary data-[state=active]:shadow-none motion-safe:animate-[config-tab-enter_300ms_ease-out_both]'
                )}
              >
                {pane.label}
              </DashboardTabTrigger>
            </Fragment>
          ))}
          {hasCollapsibleSubtabs && (
            <ConfigTabsExpandButton expanded={subtabExpanded} onClick={toggleSubtabsExpanded} />
          )}
        </DashboardTabBar>

        {visibleSubtabPanes.map((pane) => (
          <TabsContent key={pane.id} value={pane.id} className="mt-0">
            {pane.content}
          </TabsContent>
        ))}
      </Tabs>
    )
  }

  const renderTabContent = (tab: TabGroup) => {
    const tabNestedEntries = tab.sections
      .map((sectionName) => {
        const schema = configSchema.nested?.[sectionName]
        return [sectionName, schema ? getDisplaySectionSchema(sectionName, schema) : undefined] as const
      })
      .filter((entry): entry is readonly [string, ConfigSchema] => Boolean(entry[1]))

    if (tabNestedEntries.length === 0) {
      return null
    }

    if (tabNestedEntries.some(([, sectionSchema]) => sectionSchema.uiUseSubTabs)) {
      return renderSubtabbedContent(tab.id, tabNestedEntries)
    }

    const values = Object.fromEntries(
      tabNestedEntries.map(([sectionName]) => [sectionName, sectionValues[sectionName] ?? {}])
    )

    const tabSchema: ConfigSchema = {
      className: tab.id,
      classDoc: tab.label,
      fields: [],
      nested: Object.fromEntries(tabNestedEntries),
    }

    return (
      <DynamicConfigForm
        schema={tabSchema}
        values={values}
        onChange={(fieldPath, value) => {
          const [sectionName, ...restPath] = fieldPath.split('.')
          if (!sectionName) {
            return
          }

          updateSectionValueByPath(sectionName, restPath, value)
        }}
        hooks={fieldHooks}
        advancedVisible={advancedVisible}
        sectionColumns={2}
        sectionLeadingContent={sectionLeadingContent}
        sectionTrailingContent={sectionTrailingContent}
        fieldTrailingContent={fieldTrailingContent}
      />
    )
  }

  return (
    <Tabs value={activeTab} onValueChange={handleTabChange} className="w-full">
      <DashboardTabBar
        data-config-bot-tab-list="true"
        className="h-auto min-h-[3.25rem] content-start items-stretch sm:flex-wrap"
      >
        {visibleTabGroups.map((tab) => {
          const isExpandedOnlyTab = tab.advanced
          return (
            <Fragment key={tab.id}>
              <DashboardTabTrigger
                value={tab.id}
                data-config-bot-extra-tab={isExpandedOnlyTab ? 'true' : undefined}
                className={cn(
                  'min-h-9 text-lg font-semibold',
                  isExpandedOnlyTab &&
                    'overflow-hidden text-muted-foreground/80 hover:bg-background/70 data-[state=active]:bg-primary/10 data-[state=active]:text-primary data-[state=active]:shadow-none motion-safe:animate-[config-tab-enter_300ms_ease-out_both]'
                )}
              >
                {tab.label}
              </DashboardTabTrigger>
            </Fragment>
          )
        })}
        {hasCollapsibleTabs && (
          <ConfigTabsExpandButton expanded={expanded} onClick={toggleExpanded} />
        )}
        <Button
          type="button"
          variant={advancedVisible ? 'default' : 'outline'}
          size="sm"
          className="h-9 shrink-0 self-center px-2.5 text-sm leading-none transition-all duration-200 ease-out sm:ml-auto"
          onClick={() => setAdvancedVisible((current) => !current)}
        >
          高级设置
        </Button>
      </DashboardTabBar>
      {tabGuideVisible && (
        <div className="bg-muted/20 text-muted-foreground mt-2 flex flex-col gap-2 rounded-md border px-3 py-2 text-xs sm:flex-row sm:items-center sm:justify-between">
          <span>点击箭头展开隐藏配置栏目；点击“高级设置”显示高级配置项。</span>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="h-6 self-start px-2 text-xs sm:self-center"
            onClick={dismissTabGuide}
          >
            我知道了
          </Button>
        </div>
      )}
      {tabGroups.map((tab) => (
        <TabsContent
          key={tab.id}
          value={tab.id}
          className="space-y-4 motion-safe:animate-[config-tab-content-enter_180ms_ease-out_both]"
        >
          {renderTabContent(tab)}
        </TabsContent>
      ))}
      <AlertDialog
        open={experimentalNoticeOpen}
        onOpenChange={(open) => {
          if (open) {
            setExperimentalNoticeOpen(true)
            return
          }

          dismissExperimentalNotice()
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>实验性功能</AlertDialogTitle>
            <AlertDialogDescription className="leading-6">
              实验性功能指的是尚未完善、并不适用于所有麦麦、用于测试、可能移除，或可能具有有趣效果的功能选项。请斟酌开启，遇见预料之外的问题时，优先关闭实验性功能看看。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogAction onClick={dismissExperimentalNotice}>我知道了</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Tabs>
  )
}
