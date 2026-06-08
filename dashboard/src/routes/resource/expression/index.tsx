import {
  Check,
  ChevronLeft,
  ChevronRight,
  Download,
  FileClock,
  MessageSquare,
  Plus,
  Search,
  Trash2,
  Upload,
  X,
  Zap,
} from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import type { ChangeEvent } from 'react'

import { Button } from '@/components/ui/button'
import { ExpressionReviewer } from '@/components/expression-reviewer'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useToast } from '@/hooks/use-toast'

import {
  batchDeleteExpressions,
  clearExpressions,
  deleteExpression,
  exportExpressions,
  getChatList,
  getExpressionDetail,
  getExpressionGroups,
  getExpressionList,
  getExpressionStats,
  getReviewStats,
  importExpressions,
  updateExpressionReviewStatus,
} from '@/lib/expression-api'

import {
  BatchDeleteConfirmDialog,
  ClearChatExpressionsConfirmDialog,
  DeleteConfirmDialog,
  ExpressionCreateDialog,
  ExpressionDetailDialog,
  ExpressionEditDialog,
  LegacyExpressionImportDialog,
} from './ExpressionDialogs'
import { ExpressionList } from './ExpressionList'
import { ExpressionReviewLogPanel } from './ExpressionReviewLogPanel'

import type {
  ChatInfo,
  Expression,
  ExpressionExportItem,
  ExpressionGroupInfo,
} from '@/types/expression'
import type { StatsData } from './types'

type IndicatorStatus = 'on' | 'off' | 'mixed'
type ExpressionReviewFilter = 'all' | 'user_checked' | 'unchecked'
type ExpressionSortMode = 'time'

interface ExpressionLearningScopeStatus {
  label: string
  useExpression: IndicatorStatus
  enableLearning: IndicatorStatus
}

/**
 * 表达方式管理主页面
 */
export function ExpressionManagementPage() {
  const [expressions, setExpressions] = useState<Expression[]>([])
  const [loading, setLoading] = useState(true)
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [search, setSearch] = useState('')
  const [browseMode, setBrowseMode] = useState<'chat' | 'group' | 'all'>('chat')
  const [browserPanelCollapsed, setBrowserPanelCollapsed] = useState(false)
  const [showLegacyExpressions, setShowLegacyExpressions] = useState(false)
  const [reviewFilter, setReviewFilter] = useState<ExpressionReviewFilter>('all')
  const [sortMode, setSortMode] = useState<ExpressionSortMode>('time')
  const [selectedChatId, setSelectedChatId] = useState('')
  const [selectedGroupIndex, setSelectedGroupIndex] = useState<number | null>(null)
  const [selectedExpression, setSelectedExpression] = useState<Expression | null>(null)
  const [isDetailDialogOpen, setIsDetailDialogOpen] = useState(false)
  const [isEditDialogOpen, setIsEditDialogOpen] = useState(false)
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false)
  const [isLegacyImportOpen, setIsLegacyImportOpen] = useState(false)
  const [deleteConfirmExpression, setDeleteConfirmExpression] = useState<Expression | null>(null)
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set())
  const [isBatchDeleteDialogOpen, setIsBatchDeleteDialogOpen] = useState(false)
  const [isClearConfirmOpen, setIsClearConfirmOpen] = useState(false)
  const [stats, setStats] = useState<StatsData>({
    total: 0,
    recent_7days: 0,
    chat_count: 0,
    top_chats: {},
  })
  const [chatList, setChatList] = useState<ChatInfo[]>([])
  const [expressionGroups, setExpressionGroups] = useState<ExpressionGroupInfo[]>([])
  const [chatNameMap, setChatNameMap] = useState<Map<string, string>>(new Map())
  const [activeView, setActiveView] = useState<'list' | 'logs' | 'quick'>('list')
  const [uncheckedCount, setUncheckedCount] = useState(0)
  const { toast } = useToast()
  const importInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (browseMode !== 'chat') return
    if (!selectedChatId && chatList.length > 0) {
      setSelectedChatId(chatList[0].chat_id)
    }
    if (
      selectedChatId &&
      chatList.length > 0 &&
      !chatList.some((chat) => chat.chat_id === selectedChatId)
    ) {
      setSelectedChatId(chatList[0].chat_id)
    }
  }, [browseMode, chatList, selectedChatId])

  // 加载表达方式列表
  const loadExpressions = async () => {
    try {
      setLoading(true)
      const selectedGroup =
        browseMode === 'group'
          ? expressionGroups.find((group) => group.index === selectedGroupIndex)
          : undefined
      if (selectedGroup && !selectedGroup.is_global && selectedGroup.chat_ids.length === 0) {
        setExpressions([])
        setTotal(0)
        return
      }
      const result = await getExpressionList({
        page,
        page_size: pageSize,
        search: search || undefined,
        chat_id: browseMode === 'chat' ? selectedChatId || undefined : undefined,
        chat_ids: selectedGroup && !selectedGroup.is_global ? selectedGroup.chat_ids : undefined,
        include_legacy: showLegacyExpressions,
        review_filter: reviewFilter,
        sort_by: sortMode,
      })
      if (result.success) {
        setExpressions(result.data.data)
        setTotal(result.data.total)
      } else {
        toast({
          title: '加载失败',
          description: result.error,
          variant: 'destructive',
        })
      }
    } catch (error) {
      toast({
        title: '加载失败',
        description: error instanceof Error ? error.message : '无法加载表达方式',
        variant: 'destructive',
      })
    } finally {
      setLoading(false)
    }
  }

  // 加载统计数据
  const loadStats = async () => {
    try {
      const result = await getExpressionStats({ include_legacy: showLegacyExpressions })
      if (result.success) {
        setStats(result.data)
      } else {
        console.error('加载统计数据失败:', result.error)
      }
    } catch (error) {
      console.error('加载统计数据失败:', error)
    }
  }

  // 加载审核统计
  const loadReviewStats = async () => {
    try {
      const result = await getReviewStats()
      if (result.success) {
        setUncheckedCount(result.data.unchecked)
      }
    } catch (error) {
      console.error('加载审核统计失败:', error)
    }
  }

  // 加载聚天列表
  const handleActiveViewChange = (view: 'list' | 'logs' | 'quick') => {
    setActiveView(view)
    if (view === 'list') {
      loadExpressions()
      loadStats()
    }
    loadReviewStats()
  }

  const loadChatList = async () => {
    try {
      const result = await getChatList({ include_legacy: showLegacyExpressions })
      if (result.success) {
        setChatList(result.data)
        const nameMap = new Map<string, string>()
        result.data.forEach((chat: ChatInfo) => {
          nameMap.set(chat.chat_id, chat.chat_name)
        })
        setChatNameMap(nameMap)
      }
    } catch (error) {
      console.error('加载聚天列表失败:', error)
    }
  }

  // 初始加载
  const loadExpressionGroups = async () => {
    try {
      const result = await getExpressionGroups({ include_legacy: showLegacyExpressions })
      if (result.success) {
        setExpressionGroups(result.data)
      }
    } catch (error) {
      console.error('鍔犺浇琛ㄨ揪浜掗€氱粍澶辫触:', error)
    }
  }

  useEffect(() => {
    loadExpressions()
    loadReviewStats()
    loadStats()
    loadChatList()
    loadExpressionGroups()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    page,
    pageSize,
    search,
    browseMode,
    selectedChatId,
    selectedGroupIndex,
    showLegacyExpressions,
    reviewFilter,
    sortMode,
  ])

  // 查看详情
  const handleViewDetail = async (expression: Expression) => {
    try {
      const result = await getExpressionDetail(expression.id)
      if (result.success) {
        setSelectedExpression(result.data)
        setIsDetailDialogOpen(true)
      } else {
        toast({
          title: '加载详情失败',
          description: result.error,
          variant: 'destructive',
        })
      }
    } catch (error) {
      toast({
        title: '加载详情失败',
        description: error instanceof Error ? error.message : '无法加载表达方式详情',
        variant: 'destructive',
      })
    }
  }

  // 编辑表达方式
  const handleEdit = (expression: Expression) => {
    setSelectedExpression(expression)
    setIsEditDialogOpen(true)
  }

  const expressionMatchesReviewFilter = (expression: Expression) => {
    if (reviewFilter === 'all') {
      return true
    }
    if (reviewFilter === 'user_checked') {
      return expression.checked && expression.modified_by === 'user'
    }
    return !expression.checked
  }

  // 删除表达方式
  const applyReviewStatusUpdates = (
    updatedExpressions: Expression[],
    clearUpdatedSelection = false
  ) => {
    const updatedExpressionMap = new Map(
      updatedExpressions.map((expression) => [expression.id, expression])
    )
    const removedIds = new Set(
      expressions
        .filter((expression) => {
          const updatedExpression = updatedExpressionMap.get(expression.id)
          return updatedExpression && !expressionMatchesReviewFilter(updatedExpression)
        })
        .map((expression) => expression.id)
    )

    setExpressions((currentExpressions) =>
      currentExpressions.flatMap((expression) => {
        const updatedExpression = updatedExpressionMap.get(expression.id)
        if (!updatedExpression) {
          return [expression]
        }
        if (!expressionMatchesReviewFilter(updatedExpression)) {
          return []
        }
        return [updatedExpression]
      })
    )

    if (removedIds.size > 0) {
      setTotal((currentTotal) => Math.max(currentTotal - removedIds.size, 0))
    }
    if (clearUpdatedSelection || removedIds.size > 0) {
      setSelectedIds((currentSelectedIds) => {
        const nextSelectedIds = new Set(currentSelectedIds)
        updatedExpressions.forEach((expression) => {
          if (clearUpdatedSelection || removedIds.has(expression.id)) {
            nextSelectedIds.delete(expression.id)
          }
        })
        return nextSelectedIds
      })
    }
  }

  const handleDelete = async () => {
    if (!deleteConfirmExpression) return
    try {
      const result = await deleteExpression(deleteConfirmExpression.id)
      if (result.success) {
        toast({
          title: '删除成功',
          description: `已删除表达方式: ${deleteConfirmExpression.situation}`,
        })
        setDeleteConfirmExpression(null)
        loadExpressions()
        loadStats()
      } else {
        toast({
          title: '删除失败',
          description: result.error,
          variant: 'destructive',
        })
      }
    } catch (error) {
      toast({
        title: '删除失败',
        description: error instanceof Error ? error.message : '无法删除表达方式',
        variant: 'destructive',
      })
    }
  }

  // 切换单个选择
  const handleToggleReviewStatus = async (expression: Expression) => {
    const isUserApproved = expression.checked && expression.modified_by === 'user'
    const nextApproved = !isUserApproved

    try {
      const result = await updateExpressionReviewStatus(expression.id, nextApproved)
      if (result.success) {
        applyReviewStatusUpdates([result.data])
        toast({
          title: nextApproved ? '已通过' : '已拒绝',
          description: nextApproved ? '已设为人工通过' : '已取消人工通过',
        })
        loadReviewStats()
        return
      }
      toast({
        title: '更新审核状态失败',
        description: result.error,
        variant: 'destructive',
      })
    } catch (error) {
      toast({
        title: '更新审核状态失败',
        description: error instanceof Error ? error.message : '无法更新表达方式审核状态',
        variant: 'destructive',
      })
    }
  }

  const toggleSelect = (id: number) => {
    const newSelected = new Set(selectedIds)
    if (newSelected.has(id)) {
      newSelected.delete(id)
    } else {
      newSelected.add(id)
    }
    setSelectedIds(newSelected)
  }

  // 全选/取消全选
  const toggleSelectAll = () => {
    if (selectedIds.size === expressions.length && expressions.length > 0) {
      setSelectedIds(new Set())
    } else {
      setSelectedIds(new Set(expressions.map((e) => e.id)))
    }
  }

  // 批量删除
  const handleBatchDelete = async () => {
    try {
      const result = await batchDeleteExpressions(Array.from(selectedIds))
      if (result.success) {
        toast({
          title: '批量删除成功',
          description: `已删除 ${selectedIds.size} 个表达方式`,
        })
        setSelectedIds(new Set())
        setIsBatchDeleteDialogOpen(false)
        loadExpressions()
        loadStats()
      } else {
        toast({
          title: '批量删除失败',
          description: result.error,
          variant: 'destructive',
        })
      }
    } catch (error) {
      toast({
        title: '批量删除失败',
        description: error instanceof Error ? error.message : '无法批量删除表达方式',
        variant: 'destructive',
      })
    }
  }

  // 页面跳转
  const handleBatchReviewStatus = async (approved: boolean) => {
    const expressionIds = Array.from(selectedIds)
    if (expressionIds.length === 0) {
      return
    }

    try {
      const results = await Promise.all(
        expressionIds.map((expressionId) => updateExpressionReviewStatus(expressionId, approved))
      )
      const updatedExpressions = results
        .filter((result) => result.success)
        .map((result) => result.data)
      const failedCount = results.length - updatedExpressions.length

      if (updatedExpressions.length > 0) {
        applyReviewStatusUpdates(updatedExpressions, true)
        loadReviewStats()
      }

      toast({
        title: approved ? '批量设为通过完成' : '批量设为不通过完成',
        description:
          failedCount > 0
            ? `成功 ${updatedExpressions.length} 个，失败 ${failedCount} 个`
            : `已更新 ${updatedExpressions.length} 个表达方式`,
        variant: failedCount > 0 ? 'destructive' : undefined,
      })
    } catch (error) {
      toast({
        title: '批量更新审核状态失败',
        description: error instanceof Error ? error.message : '无法批量更新表达方式审核状态',
        variant: 'destructive',
      })
    }
  }

  const handleJumpToPage = (jumpToPage: string) => {
    const targetPage = parseInt(jumpToPage)
    const totalPages = Math.ceil(total / pageSize)
    if (targetPage >= 1 && targetPage <= totalPages) {
      setPage(targetPage)
    }
  }

  const handleChatChange = (chatId: string) => {
    setSelectedChatId(chatId)
    setPage(1)
    setSelectedIds(new Set())
  }

  const handleBrowseModeChange = (mode: 'chat' | 'group' | 'all') => {
    setBrowseMode(mode)
    if (mode === 'chat' && !selectedChatId && chatList.length > 0) {
      setSelectedChatId(chatList[0].chat_id)
    }
    if (mode !== 'chat') {
      setSelectedChatId('')
    }
    if (mode === 'group' && selectedGroupIndex === null && expressionGroups.length > 0) {
      setSelectedGroupIndex(expressionGroups[0].index)
    }
    if (mode !== 'group') {
      setSelectedGroupIndex(null)
    }
    setPage(1)
    setSelectedIds(new Set())
  }

  const handleGroupChange = (groupIndex: number | null) => {
    setSelectedGroupIndex(groupIndex)
    setPage(1)
    setSelectedIds(new Set())
  }

  const handleToggleLegacyExpressions = () => {
    setShowLegacyExpressions((current) => !current)
    setSelectedChatId('')
    setSelectedGroupIndex(null)
    setPage(1)
    setSelectedIds(new Set())
  }

  const summarizeStatus = (values: boolean[]): IndicatorStatus => {
    if (values.every(Boolean)) {
      return 'on'
    }
    if (values.every((value) => !value)) {
      return 'off'
    }
    return 'mixed'
  }

  const getScopeStatus = (): ExpressionLearningScopeStatus | null => {
    if (browseMode === 'chat' && selectedChatId) {
      const selectedChat = chatList.find((chat) => chat.chat_id === selectedChatId)
      if (!selectedChat) {
        return null
      }
      return {
        label: selectedChat.chat_name,
        useExpression: selectedChat.use_expression ? 'on' : 'off',
        enableLearning: selectedChat.enable_learning ? 'on' : 'off',
      }
    }

    if (browseMode === 'group' && selectedGroupIndex !== null) {
      const selectedGroup = expressionGroups.find((group) => group.index === selectedGroupIndex)
      if (!selectedGroup || selectedGroup.members.length === 0) {
        return null
      }
      return {
        label: selectedGroup.name,
        useExpression: summarizeStatus(
          selectedGroup.members.map((member) => member.use_expression)
        ),
        enableLearning: summarizeStatus(
          selectedGroup.members.map((member) => member.enable_learning)
        ),
      }
    }

    return null
  }

  const scopeStatus = getScopeStatus()
  const renderStatusIndicator = (label: string, status: IndicatorStatus) => {
    const statusText = status === 'mixed' ? '部分开启' : status === 'on' ? '已开启' : '已关闭'
    const dotClass =
      status === 'mixed' ? 'bg-amber-500' : status === 'on' ? 'bg-green-500' : 'bg-muted-foreground'

    return (
      <div className="bg-background flex items-center gap-2 rounded-md border px-3 py-2 text-sm sm:py-1.5">
        <span className={`h-2.5 w-2.5 rounded-full ${dotClass}`} />
        <span className="text-muted-foreground">{label}</span>
        <span className="font-medium">{statusText}</span>
      </div>
    )
  }

  const currentChat =
    browseMode === 'chat' && selectedChatId
      ? chatList.find((chat) => chat.chat_id === selectedChatId)
      : null

  const getImportExportChatId = (): string | null => {
    if (!currentChat) {
      toast({
        title: '请选择聊天',
        description: '表达方式导入导出需要先在左侧选择一个具体聊天',
        variant: 'destructive',
      })
      return null
    }
    return currentChat.chat_id
  }

  const downloadJson = (filename: string, data: unknown) => {
    const blob = new Blob([JSON.stringify(data, null, 2)], {
      type: 'application/json;charset=utf-8',
    })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    document.body.appendChild(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
  }

  const sanitizeFilename = (name: string) => {
    return name.replace(/[\\/:*?"<>|]/g, '_').slice(0, 60) || 'chat'
  }

  const handleExportExpressions = async (onlySelected: boolean) => {
    const chatId = getImportExportChatId()
    if (!chatId || !currentChat) return
    if (onlySelected && selectedIds.size === 0) {
      toast({
        title: '没有选中项目',
        description: '请先选择要导出的表达方式',
        variant: 'destructive',
      })
      return
    }

    const result = await exportExpressions({
      chat_id: chatId,
      ids: onlySelected ? Array.from(selectedIds) : undefined,
    })
    if (!result.success) {
      toast({
        title: '导出失败',
        description: result.error,
        variant: 'destructive',
      })
      return
    }

    const filename = `expressions-${sanitizeFilename(currentChat.chat_name)}-${onlySelected ? 'selected' : 'all'}.json`
    downloadJson(filename, result.data)
    toast({
      title: '导出成功',
      description: `已导出 ${result.data.count} 个表达方式`,
    })
  }

  const normalizeImportItems = (payload: unknown): ExpressionExportItem[] => {
    if (Array.isArray(payload)) {
      return payload as ExpressionExportItem[]
    }
    if (
      payload &&
      typeof payload === 'object' &&
      Array.isArray((payload as { expressions?: unknown }).expressions)
    ) {
      return (payload as { expressions: ExpressionExportItem[] }).expressions
    }
    return []
  }

  const handleImportFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const chatId = getImportExportChatId()
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!chatId || !file) return

    try {
      const payload = JSON.parse(await file.text()) as unknown
      const expressionsToImport = normalizeImportItems(payload)
      if (expressionsToImport.length === 0) {
        toast({
          title: '导入失败',
          description: 'JSON 中没有可导入的表达方式',
          variant: 'destructive',
        })
        return
      }

      const result = await importExpressions({
        chat_id: chatId,
        expressions: expressionsToImport,
      })
      if (!result.success) {
        toast({
          title: '导入失败',
          description: result.error,
          variant: 'destructive',
        })
        return
      }

      toast({
        title: '导入成功',
        description: `成功 ${result.data.imported_count} 个，跳过 ${result.data.skipped_count} 个，失败 ${result.data.failed_count} 个`,
      })
      loadExpressions()
      loadStats()
      loadReviewStats()
    } catch (error) {
      toast({
        title: '导入失败',
        description: error instanceof Error ? error.message : '无法解析 JSON 文件',
        variant: 'destructive',
      })
    }
  }

  const handleClearCurrentChat = async () => {
    const chatId = getImportExportChatId()
    if (!chatId) return

    const result = await clearExpressions({ chat_id: chatId })
    if (!result.success) {
      toast({
        title: '清除失败',
        description: result.error,
        variant: 'destructive',
      })
      return
    }

    toast({
      title: '清除成功',
      description: result.data.message,
    })
    setSelectedIds(new Set())
    setIsClearConfirmOpen(false)
    loadExpressions()
    loadStats()
    loadReviewStats()
  }

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden p-4 pb-6 sm:p-6">
      <div className="mb-4 flex shrink-0 flex-col gap-3 sm:mb-3">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="-mx-1 w-[calc(100%+0.5rem)] overflow-x-auto px-1 pb-1 sm:mx-0 sm:w-auto sm:overflow-visible sm:p-0">
            <div className="bg-muted inline-flex w-max min-w-full rounded-lg border p-1 sm:w-fit sm:min-w-0">
              <button
                type="button"
                onClick={() => handleActiveViewChange('list')}
                className={`inline-flex h-10 flex-1 shrink-0 items-center justify-center gap-2 rounded-md px-3 text-sm font-medium transition-colors sm:h-8 sm:flex-none ${
                  activeView === 'list'
                    ? 'bg-background text-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                <MessageSquare className="h-4 w-4" />
                <span>表达</span>
              </button>
              <button
                type="button"
                onClick={() => handleActiveViewChange('quick')}
                className={`inline-flex h-10 flex-1 shrink-0 items-center justify-center gap-2 rounded-md px-3 text-sm font-medium transition-colors sm:h-8 sm:flex-none ${
                  activeView === 'quick'
                    ? 'bg-background text-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                <Zap className="h-4 w-4" />
                <span>快速审核</span>
                {uncheckedCount > 0 && (
                  <span className="ml-0.5 rounded-full bg-orange-500 px-1.5 py-0.5 text-xs leading-none text-white">
                    {uncheckedCount > 99 ? '99+' : uncheckedCount}
                  </span>
                )}
              </button>
            </div>
          </div>
          {activeView === 'list' && (
            <div className="grid grid-cols-1 gap-2 sm:flex sm:items-center lg:justify-end">
              <Button
                variant="outline"
                onClick={() => handleActiveViewChange('logs')}
                className="h-10 justify-center gap-2 sm:h-9"
              >
                <FileClock className="h-4 w-4" />
                AI审核记录
              </Button>
              <Button
                onClick={() => setIsCreateDialogOpen(true)}
                className="h-10 justify-center gap-2 sm:h-9"
              >
                <Plus className="h-4 w-4" />
                新增表达方式
              </Button>
              <Button
                variant="outline"
                onClick={() => setIsLegacyImportOpen(true)}
                className="h-10 justify-center gap-2 sm:h-9"
              >
                <Upload className="h-4 w-4" />
                从旧版本导入
              </Button>
            </div>
          )}
        </div>
      </div>

      <ScrollArea className={activeView === 'list' ? 'min-h-0 flex-1' : 'hidden'}>
        <div className="space-y-5 pr-3 pb-2 sm:space-y-6 sm:pr-4">
          {/* 搜索和批量操作 */}
          <div className="flex flex-col gap-3 lg:flex-row lg:items-start">
            <div className="bg-card/80 grid w-full grid-cols-3 overflow-hidden rounded-md border sm:h-9 lg:w-[24rem] lg:flex-none">
              <div className="flex min-h-11 min-w-0 flex-col items-center justify-center gap-1 px-2 text-center sm:min-h-0 sm:flex-row sm:justify-between sm:gap-2 sm:px-3 sm:text-left">
                <div className="text-muted-foreground text-[11px] sm:text-xs">总数量</div>
                <div className="text-sm leading-none font-semibold sm:text-base">{stats.total}</div>
              </div>
              <div className="flex min-h-11 min-w-0 flex-col items-center justify-center gap-1 border-l px-2 text-center sm:min-h-0 sm:flex-row sm:justify-between sm:gap-2 sm:px-3 sm:text-left">
                <div className="text-muted-foreground text-[11px] sm:text-xs">近7天新增</div>
                <div className="text-sm leading-none font-semibold text-green-600 sm:text-base">
                  {stats.recent_7days}
                </div>
              </div>
              <div className="flex min-h-11 min-w-0 flex-col items-center justify-center gap-1 border-l px-2 text-center sm:min-h-0 sm:flex-row sm:justify-between sm:gap-2 sm:px-3 sm:text-left">
                <div className="text-muted-foreground text-[11px] sm:text-xs">关联聊天数</div>
                <div className="text-sm leading-none font-semibold text-blue-600 sm:text-base">
                  {stats.chat_count}
                </div>
              </div>
            </div>

            <div className="w-full lg:min-w-0 lg:flex-1">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-2">
                <div className="min-w-0 flex-1">
                  <div className="relative">
                    <Search className="text-muted-foreground absolute top-3 left-3 h-4 w-4 sm:top-2 sm:left-2.5" />
                    <Input
                      id="search"
                      aria-label="搜索"
                      placeholder="搜索情境、风格或上下文..."
                      value={search}
                      onChange={(e) => setSearch(e.target.value)}
                      className="h-10 pl-10 sm:h-8 sm:pl-9"
                    />
                  </div>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-muted-foreground h-9 px-3 text-xs sm:h-8 sm:px-2"
                    title="显示旧格式的表达方式（这些项目会在运行中被转换为新格式）"
                    onClick={handleToggleLegacyExpressions}
                  >
                    {showLegacyExpressions ? '隐藏旧格式' : '显示旧格式'}
                  </Button>
                  <Label htmlFor="page-size" className="text-sm whitespace-nowrap">
                    每页显示
                  </Label>
                  <Select
                    value={pageSize.toString()}
                    onValueChange={(value) => {
                      setPageSize(parseInt(value))
                      setPage(1)
                      setSelectedIds(new Set())
                    }}
                  >
                    <SelectTrigger id="page-size" className="h-9 w-20 sm:h-8">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="10">10</SelectItem>
                      <SelectItem value="20">20</SelectItem>
                      <SelectItem value="50">50</SelectItem>
                      <SelectItem value="100">100</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* 批量操作工具栏 */}
              <div
                className={`${selectedIds.size > 0 ? 'flex' : 'hidden'} mt-4 flex-col items-start justify-between gap-3 border-t pt-4 sm:mt-3 sm:flex-row sm:items-center sm:pt-3`}
              >
                <div className="text-muted-foreground flex items-center gap-2 text-sm">
                  {selectedIds.size > 0 && <span>已选择 {selectedIds.size} 个表达方式</span>}
                </div>
                <div className="grid w-full grid-cols-2 gap-2 sm:flex sm:w-auto sm:flex-wrap sm:items-center">
                  {selectedIds.size > 0 && (
                    <>
                      <Button variant="outline" size="sm" onClick={() => setSelectedIds(new Set())}>
                        取消选择
                      </Button>
                      <Button
                        variant="default"
                        size="sm"
                        className="gap-1"
                        onClick={() => handleBatchReviewStatus(true)}
                      >
                        <Check className="h-4 w-4" />
                        批量通过
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="gap-1"
                        onClick={() => handleBatchReviewStatus(false)}
                      >
                        <X className="h-4 w-4" />
                        批量不通过
                      </Button>
                      <Button
                        variant="destructive"
                        size="sm"
                        onClick={() => setIsBatchDeleteDialogOpen(true)}
                      >
                        <Trash2 className="mr-1 h-4 w-4" />
                        批量删除
                      </Button>
                    </>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* 表达方式列表 */}
          <div
            className={`grid grid-cols-1 gap-5 transition-[grid-template-columns] duration-200 sm:gap-4 ${
              browserPanelCollapsed
                ? 'lg:grid-cols-[3.25rem_minmax(0,1fr)]'
                : 'lg:grid-cols-[16rem_minmax(0,1fr)]'
            }`}
          >
            <aside className="bg-card rounded-lg border lg:sticky lg:top-0 lg:flex lg:max-h-[calc(100vh-18rem)] lg:flex-col lg:overflow-hidden">
              <div className="border-b px-4 py-3 sm:px-3 sm:py-2">
                <div className="grid w-64 grid-cols-[2rem_minmax(0,1fr)] items-center gap-2">
                  <button
                    type="button"
                    onClick={() => setBrowserPanelCollapsed((collapsed) => !collapsed)}
                    className="text-muted-foreground hover:bg-muted hover:text-foreground flex h-8 w-8 items-center justify-center rounded-md transition-colors"
                    aria-label={browserPanelCollapsed ? '展开浏览列表' : '折叠浏览列表'}
                    aria-expanded={!browserPanelCollapsed}
                    title={browserPanelCollapsed ? '展开浏览列表' : '折叠浏览列表'}
                  >
                    {browserPanelCollapsed ? (
                      <ChevronRight className="h-4 w-4" />
                    ) : (
                      <ChevronLeft className="h-4 w-4" />
                    )}
                  </button>
                  <div className="bg-muted grid grid-cols-3 gap-1 rounded-md p-1.5 sm:p-1">
                    <button
                      type="button"
                      onClick={() => handleBrowseModeChange('chat')}
                      className={`rounded px-2 py-2 text-xs transition-colors sm:py-1 ${
                        browseMode === 'chat'
                          ? 'bg-background shadow-sm'
                          : 'text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      按聊天
                    </button>
                    <button
                      type="button"
                      onClick={() => handleBrowseModeChange('group')}
                      className={`rounded px-2 py-2 text-xs transition-colors sm:py-1 ${
                        browseMode === 'group'
                          ? 'bg-background shadow-sm'
                          : 'text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      按互通组
                    </button>
                    <button
                      type="button"
                      onClick={() => handleBrowseModeChange('all')}
                      className={`rounded px-2 py-2 text-xs transition-colors sm:py-1 ${
                        browseMode === 'all'
                          ? 'bg-background shadow-sm'
                          : 'text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      全部
                    </button>
                  </div>
                </div>
              </div>
              <div className="max-h-72 w-64 space-y-2 overflow-y-auto p-3 sm:max-h-56 sm:space-y-1 sm:p-2 lg:max-h-none lg:min-h-0 lg:flex-1">
                {browseMode === 'chat' ? (
                  <>
                    {chatList.map((chat) => (
                      <button
                        key={chat.chat_id}
                        type="button"
                        onClick={() => handleChatChange(chat.chat_id)}
                        className={`w-full rounded-md px-3 py-2.5 text-left text-sm transition-colors sm:px-2 sm:py-2 ${
                          selectedChatId === chat.chat_id
                            ? 'bg-primary text-primary-foreground'
                            : 'text-foreground hover:bg-muted'
                        }`}
                        title={`${chat.chat_name} (${chat.chat_id})`}
                      >
                        <span className="block truncate">{chat.chat_name}</span>
                        <span
                          className={`block truncate text-xs ${
                            selectedChatId === chat.chat_id
                              ? 'text-primary-foreground/75'
                              : 'text-muted-foreground'
                          }`}
                        >
                          {chat.chat_id}
                        </span>
                      </button>
                    ))}
                  </>
                ) : browseMode === 'group' ? (
                  <>
                    {expressionGroups.map((group) => (
                      <button
                        key={group.index}
                        type="button"
                        onClick={() => handleGroupChange(group.index)}
                        className={`w-full rounded-md px-3 py-2.5 text-left text-sm transition-colors sm:px-2 sm:py-2 ${
                          selectedGroupIndex === group.index
                            ? 'bg-primary text-primary-foreground'
                            : 'text-foreground hover:bg-muted'
                        }`}
                        title={group.members.map((member) => member.chat_name).join('、')}
                      >
                        <span className="block truncate">
                          {group.name}
                          {group.is_global ? '（全局）' : ''}
                        </span>
                        <span
                          className={`block truncate text-xs ${
                            selectedGroupIndex === group.index
                              ? 'text-primary-foreground/75'
                              : 'text-muted-foreground'
                          }`}
                        >
                          {group.members.length > 0
                            ? group.members.map((member) => member.chat_name).join('、')
                            : '暂无已解析聊天'}
                        </span>
                      </button>
                    ))}
                    {expressionGroups.length === 0 && (
                      <div className="text-muted-foreground px-2 py-6 text-center text-sm">
                        暂无互通组
                      </div>
                    )}
                  </>
                ) : (
                  <div className="text-muted-foreground px-2 py-6 text-center text-sm">
                    当前显示全部表达方式
                  </div>
                )}
              </div>
            </aside>

            <div className="space-y-4 sm:space-y-3">
              {scopeStatus && (
                <div className="bg-card flex flex-wrap items-center gap-3 rounded-lg border px-4 py-3 sm:gap-2 sm:px-3 sm:py-2">
                  <div className="min-w-0 text-sm font-medium sm:mr-2">
                    <span className="text-muted-foreground">当前范围：</span>
                    <span>{scopeStatus.label}</span>
                  </div>
                  {renderStatusIndicator('开启学习', scopeStatus.enableLearning)}
                  {renderStatusIndicator('开启使用', scopeStatus.useExpression)}
                  <div className="grid w-full grid-cols-2 gap-2 sm:flex sm:w-auto sm:items-center">
                    <div className="flex items-center gap-2">
                      <span className="text-muted-foreground text-sm">筛选</span>
                      <Select
                        value={reviewFilter}
                        onValueChange={(value) => {
                          setReviewFilter(value as ExpressionReviewFilter)
                          setPage(1)
                        }}
                      >
                        <SelectTrigger className="h-9 w-full min-w-[8.5rem] sm:h-8 sm:w-[8.5rem]">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="all">全部</SelectItem>
                          <SelectItem value="user_checked">仅人工通过</SelectItem>
                          <SelectItem value="unchecked">未人工检查</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-muted-foreground text-sm">排序</span>
                      <Select
                        value={sortMode}
                        onValueChange={(value) => {
                          setSortMode(value as ExpressionSortMode)
                          setPage(1)
                        }}
                      >
                        <SelectTrigger className="h-9 w-full min-w-[7rem] sm:h-8 sm:w-28">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="time">按时间</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  {currentChat && (
                    <div className="grid w-full grid-cols-2 gap-2 sm:ml-auto sm:flex sm:w-auto sm:flex-wrap sm:items-center">
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-9 justify-center gap-1 sm:h-8"
                        onClick={() => handleExportExpressions(false)}
                      >
                        <Download className="h-4 w-4" />
                        导出全部
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-9 justify-center gap-1 sm:h-8"
                        onClick={() => handleExportExpressions(true)}
                        disabled={selectedIds.size === 0}
                      >
                        <Download className="h-4 w-4" />
                        导出所选
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-9 justify-center gap-1 sm:h-8"
                        onClick={() => importInputRef.current?.click()}
                      >
                        <Upload className="h-4 w-4" />
                        导入 JSON
                      </Button>
                      <Button
                        variant="destructive"
                        size="sm"
                        className="h-9 justify-center sm:h-8"
                        onClick={() => setIsClearConfirmOpen(true)}
                      >
                        清除
                      </Button>
                      <input
                        ref={importInputRef}
                        type="file"
                        accept="application/json,.json"
                        className="hidden"
                        onChange={handleImportFileChange}
                      />
                    </div>
                  )}
                </div>
              )}

              <ExpressionList
                expressions={expressions}
                loading={loading}
                total={total}
                page={page}
                pageSize={pageSize}
                selectedIds={selectedIds}
                chatNameMap={chatNameMap}
                hideChatColumn={
                  (browseMode === 'chat' && selectedChatId !== '') ||
                  (browseMode === 'group' && selectedGroupIndex !== null)
                }
                onEdit={handleEdit}
                onViewDetail={handleViewDetail}
                onDelete={(expression) => setDeleteConfirmExpression(expression)}
                onToggleReviewStatus={handleToggleReviewStatus}
                onToggleSelect={toggleSelect}
                onToggleSelectAll={toggleSelectAll}
                onPageChange={setPage}
                onJumpToPage={handleJumpToPage}
              />
            </div>
          </div>
        </div>
      </ScrollArea>

      {activeView === 'logs' && (
        <div className="min-h-[38rem] flex-1 pr-4">
          <ExpressionReviewLogPanel
            onRescued={() => {
              loadExpressions()
              loadStats()
              loadReviewStats()
            }}
          />
        </div>
      )}

      {activeView === 'quick' && (
        <div className="min-h-[38rem] flex-1 pr-4">
          <ExpressionReviewer
            embedded
            open
            mode="quick"
            className="h-full"
            onReviewed={() => {
              loadExpressions()
              loadStats()
              loadReviewStats()
            }}
          />
        </div>
      )}

      {/* 详情对话框 */}
      <ExpressionDetailDialog
        expression={selectedExpression}
        open={isDetailDialogOpen}
        onOpenChange={setIsDetailDialogOpen}
        chatNameMap={chatNameMap}
      />

      {/* 创建对话框 */}
      <ExpressionCreateDialog
        open={isCreateDialogOpen}
        onOpenChange={setIsCreateDialogOpen}
        chatList={chatList}
        onSuccess={() => {
          loadExpressions()
          loadStats()
          setIsCreateDialogOpen(false)
        }}
      />

      {/* 编辑对话框 */}
      <ExpressionEditDialog
        expression={selectedExpression}
        open={isEditDialogOpen}
        onOpenChange={setIsEditDialogOpen}
        chatList={chatList}
        onSuccess={() => {
          loadExpressions()
          loadStats()
          setIsEditDialogOpen(false)
        }}
      />

      <LegacyExpressionImportDialog
        open={isLegacyImportOpen}
        onOpenChange={setIsLegacyImportOpen}
        chatList={chatList}
        onSuccess={() => {
          loadExpressions()
          loadStats()
          loadReviewStats()
          loadChatList()
          loadExpressionGroups()
        }}
      />

      {/* 删除确认对话框 */}
      <DeleteConfirmDialog
        expression={deleteConfirmExpression}
        open={!!deleteConfirmExpression}
        onOpenChange={() => setDeleteConfirmExpression(null)}
        onConfirm={handleDelete}
      />

      {/* 批量删除确认对话框 */}
      <BatchDeleteConfirmDialog
        open={isBatchDeleteDialogOpen}
        onOpenChange={setIsBatchDeleteDialogOpen}
        onConfirm={handleBatchDelete}
        count={selectedIds.size}
      />

      <ClearChatExpressionsConfirmDialog
        open={isClearConfirmOpen}
        onOpenChange={setIsClearConfirmOpen}
        chatName={currentChat?.chat_name || ''}
        onConfirm={handleClearCurrentChat}
      />
    </div>
  )
}
