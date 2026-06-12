import { Check, Plus, Search, Trash2, X } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { Button } from '@/components/ui/button'
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
  batchDeleteJargons,
  batchSetJargonStatus,
  deleteJargon,
  getJargonChatList,
  getJargonDetail,
  getJargonList,
  getJargonStats,
} from '@/lib/jargon-api'

import {
  BatchDeleteConfirmDialog,
  DeleteConfirmDialog,
  JargonCreateDialog,
  JargonDetailDialog,
  JargonEditDialog,
} from './JargonDialogs'
import { JargonList } from './JargonList'

import type { Jargon, JargonChatInfo } from '@/types/jargon'
import type { StatsData } from './types'

/**
 * 黑话管理主页面
 */
export function JargonManagementPage() {
  const [jargons, setJargons] = useState<Jargon[]>([])
  const [loading, setLoading] = useState(true)
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [scopeFilter, setScopeFilter] = useState<'all' | 'global' | 'local'>('all')
  const [filterChatId, setFilterChatId] = useState<string>('all')
  const [filterIsJargon, setFilterIsJargon] = useState<string>('all')
  const [selectedJargon, setSelectedJargon] = useState<Jargon | null>(null)
  const [isDetailDialogOpen, setIsDetailDialogOpen] = useState(false)
  const [isEditDialogOpen, setIsEditDialogOpen] = useState(false)
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false)
  const [deleteConfirmJargon, setDeleteConfirmJargon] = useState<Jargon | null>(null)
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set())
  const [isBatchDeleteDialogOpen, setIsBatchDeleteDialogOpen] = useState(false)
  const [stats, setStats] = useState<StatsData>({
    total: 0,
    confirmed_jargon: 0,
    confirmed_not_jargon: 0,
    pending: 0,
    global_count: 0,
    complete_count: 0,
    chat_count: 0,
    top_chats: {},
  })
  const [chatList, setChatList] = useState<JargonChatInfo[]>([])
  const [formChatList, setFormChatList] = useState<JargonChatInfo[]>([])
  const jargonListRequestSeqRef = useRef(0)
  const { toast } = useToast()

  // 加载黑话列表
  const loadJargons = async () => {
    const requestSeq = jargonListRequestSeqRef.current + 1
    jargonListRequestSeqRef.current = requestSeq
    try {
      setLoading(true)
      const response = await getJargonList({
        page,
        page_size: pageSize,
        search: debouncedSearch || undefined,
        session_id: scopeFilter !== 'global' && filterChatId !== 'all' ? filterChatId : undefined,
        is_jargon: filterIsJargon === 'all' ? undefined : filterIsJargon === 'true' ? true : filterIsJargon === 'false' ? false : undefined,
        is_global: scopeFilter === 'all' ? undefined : scopeFilter === 'global',
      })
      if (requestSeq !== jargonListRequestSeqRef.current) {
        return
      }
      setJargons(response.data)
      setTotal(response.total)
    } catch (error) {
      if (requestSeq !== jargonListRequestSeqRef.current) {
        return
      }
      toast({
        title: '加载失败',
        description: error instanceof Error ? error.message : '无法加载黑话列表',
        variant: 'destructive',
      })
    } finally {
      if (requestSeq === jargonListRequestSeqRef.current) {
        setLoading(false)
      }
    }
  }

  // 加载统计数据
  const loadStats = async () => {
    try {
      const response = await getJargonStats()
      if (response?.data) {
        setStats(response.data)
      }
    } catch (error) {
      console.error('加载统计数据失败:', error)
    }
  }

  // 加载聊天列表
  const loadChatList = async () => {
    try {
      const [sidebarResponse, formResponse] = await Promise.all([
        getJargonChatList(),
        getJargonChatList({ include_empty: true }),
      ])
      if (sidebarResponse?.data) {
        setChatList(sidebarResponse.data)
      }
      if (formResponse?.data) {
        setFormChatList(formResponse.data)
      }
    } catch (error) {
      console.error('加载聊天列表失败:', error)
    }
  }

  useEffect(() => {
    const timerId = window.setTimeout(() => {
      const normalizedSearch = search.trim()
      setDebouncedSearch((current) => (current === normalizedSearch ? current : normalizedSearch))
      setPage((current) => (current === 1 ? current : 1))
      setSelectedIds((current) => (current.size === 0 ? current : new Set<number>()))
    }, 300)

    return () => window.clearTimeout(timerId)
  }, [search])

  useEffect(() => {
    loadJargons()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page, pageSize, debouncedSearch, scopeFilter, filterChatId, filterIsJargon])

  useEffect(() => {
    loadStats()
    loadChatList()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // 查看详情
  const handleViewDetail = async (jargon: Jargon) => {
    try {
      const response = await getJargonDetail(jargon.id)
      setSelectedJargon(response.data)
      setIsDetailDialogOpen(true)
    } catch (error) {
      toast({
        title: '加载详情失败',
        description: error instanceof Error ? error.message : '无法加载黑话详情',
        variant: 'destructive',
      })
    }
  }

  // 编辑黑话
  const handleEdit = (jargon: Jargon) => {
    setSelectedJargon(jargon)
    setIsEditDialogOpen(true)
  }

  // 删除黑话
  const handleDelete = async () => {
    if (!deleteConfirmJargon) return
    try {
      await deleteJargon(deleteConfirmJargon.id)
      toast({
        title: '删除成功',
        description: `已删除黑话: ${deleteConfirmJargon.content}`,
      })
      setDeleteConfirmJargon(null)
      loadJargons()
      loadStats()
      loadChatList()
    } catch (error) {
      toast({
        title: '删除失败',
        description: error instanceof Error ? error.message : '无法删除黑话',
        variant: 'destructive',
      })
    }
  }

  // 切换单个选择
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
    if (selectedIds.size === jargons.length && jargons.length > 0) {
      setSelectedIds(new Set())
    } else {
      setSelectedIds(new Set(jargons.map(j => j.id)))
    }
  }

  // 批量删除
  const handleBatchDelete = async () => {
    try {
      await batchDeleteJargons(Array.from(selectedIds))
      toast({
        title: '批量删除成功',
        description: `已删除 ${selectedIds.size} 个黑话`,
      })
      setSelectedIds(new Set())
      setIsBatchDeleteDialogOpen(false)
      loadJargons()
      loadStats()
      loadChatList()
    } catch (error) {
      toast({
        title: '批量删除失败',
        description: error instanceof Error ? error.message : '无法批量删除黑话',
        variant: 'destructive',
      })
    }
  }

  // 批量设置为黑话
  const handleBatchSetJargon = async (isJargon: boolean) => {
    try {
      await batchSetJargonStatus(Array.from(selectedIds), isJargon)
      toast({
        title: '操作成功',
        description: `已将 ${selectedIds.size} 个词条设为${isJargon ? '黑话' : '非黑话'}`,
      })
      setSelectedIds(new Set())
      loadJargons()
      loadStats()
    } catch (error) {
      toast({
        title: '操作失败',
        description: error instanceof Error ? error.message : '批量设置失败',
        variant: 'destructive',
      })
    }
  }

  // 页面跳转
  const handleJumpToPage = (jumpToPage: string) => {
    const targetPage = parseInt(jumpToPage)
    const totalPages = Math.ceil(total / pageSize)
    if (targetPage >= 1 && targetPage <= totalPages) {
      setPage(targetPage)
    } else {
      toast({
        title: '无效的页码',
        description: `请输入1-${totalPages}之间的页码`,
        variant: 'destructive',
      })
    }
  }

  const handleChatChange = (chatId: string) => {
    setFilterChatId(chatId)
    setPage(1)
    setSelectedIds(new Set())
  }

  const handleScopeChange = (scope: 'all' | 'global' | 'local') => {
    setScopeFilter(scope)
    if (scope === 'global') {
      setFilterChatId('all')
    }
    setPage(1)
    setSelectedIds(new Set())
  }

  return (
    <div className="h-[calc(100vh-4rem)] flex flex-col p-4 sm:p-6">
      <ScrollArea className="flex-1">
        <div className="space-y-4 sm:space-y-6 pr-4">

          {/* 统计标签 */}
          <div className="grid grid-cols-2 overflow-hidden rounded-md border bg-muted/40 sm:grid-cols-4 lg:grid-cols-7">
            {[
              { label: '总数量', value: stats.total, className: 'text-foreground' },
              { label: '已确认黑话', value: stats.confirmed_jargon, className: 'text-green-600' },
              { label: '确认非黑话', value: stats.confirmed_not_jargon, className: 'text-gray-500' },
              { label: '待判定', value: stats.pending, className: 'text-yellow-600' },
              { label: '全局黑话', value: stats.global_count, className: 'text-blue-600' },
              { label: '推断完成', value: stats.complete_count, className: 'text-purple-600' },
              { label: '关联聊天数', value: stats.chat_count, className: 'text-foreground' },
            ].map((item, index) => (
              <div
                key={item.label}
                className={`flex h-9 min-w-0 items-center justify-center gap-2 px-3 text-sm ${
                  index % 2 === 1 ? 'border-l' : ''
                } ${
                  index >= 2 ? 'border-t sm:border-t-0' : ''
                } ${
                  index > 0 ? 'sm:border-l' : ''
                } ${
                  index >= 4 ? 'sm:border-t lg:border-t-0' : ''
                }`}
              >
                <span className="truncate text-muted-foreground">{item.label}</span>
                <span className={`shrink-0 font-semibold leading-none ${item.className}`}>
                  {item.value}
                </span>
              </div>
            ))}
          </div>

          {/* 搜索和筛选 */}
          <div className="rounded-lg border bg-card p-3">
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,1fr)_auto] lg:items-end">
              <div className="space-y-1">
                <Label htmlFor="search">搜索</Label>
                <div className="relative">
                  <Search className="absolute left-2.5 top-2 h-4 w-4 text-muted-foreground" />
                  <Input
                    id="search"
                    placeholder="搜索黑话内容..."
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    className="h-8 pl-9"
                  />
                </div>
              </div>
              <div className="space-y-1">
                <Label>状态筛选</Label>
                <Select value={filterIsJargon} onValueChange={setFilterIsJargon}>
                  <SelectTrigger className="h-8">
                    <SelectValue placeholder="全部状态" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">全部状态</SelectItem>
                    <SelectItem value="true">是黑话</SelectItem>
                    <SelectItem value="false">非黑话</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1">
                <Label htmlFor="page-size">每页显示</Label>
                <Select
                  value={pageSize.toString()}
                  onValueChange={(value) => {
                    setPageSize(parseInt(value))
                    setPage(1)
                    setSelectedIds(new Set())
                  }}
                >
                  <SelectTrigger id="page-size" className="h-8">
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
              <Button onClick={() => setIsCreateDialogOpen(true)} className="h-8 gap-2">
                <Plus className="h-4 w-4" />
                新增黑话
              </Button>
            </div>

            {/* 批量操作工具栏 */}
            {selectedIds.size > 0 && (
              <div className="flex flex-wrap items-center gap-2 mt-4 pt-4 border-t">
                <span className="text-sm text-muted-foreground">已选择 {selectedIds.size} 个</span>
                <Button variant="outline" size="sm" onClick={() => handleBatchSetJargon(true)}>
                  <Check className="h-4 w-4 mr-1" />
                  标记为黑话
                </Button>
                <Button variant="outline" size="sm" onClick={() => handleBatchSetJargon(false)}>
                  <X className="h-4 w-4 mr-1" />
                  标记为非黑话
                </Button>
                <Button variant="outline" size="sm" onClick={() => setSelectedIds(new Set())}>
                  取消选择
                </Button>
                <Button variant="destructive" size="sm" onClick={() => setIsBatchDeleteDialogOpen(true)}>
                  <Trash2 className="h-4 w-4 mr-1" />
                  批量删除
                </Button>
              </div>
            )}
          </div>

          {/* 黑话列表 */}
          <div className="grid grid-cols-1 gap-4 lg:h-[calc(100vh-19rem)] lg:min-h-[30rem] lg:grid-cols-[12rem_minmax(0,1fr)] lg:items-stretch">
            <aside className="flex min-h-0 flex-col rounded-lg border bg-card lg:h-full lg:self-stretch lg:overflow-hidden">
              <div className="space-y-2 border-b px-3 py-2">
                <h2 className="text-sm font-medium">范围</h2>
                <div className="grid grid-cols-3 gap-1 rounded-md bg-muted p-1">
                  <button
                    type="button"
                    onClick={() => handleScopeChange('all')}
                    className={`rounded px-2 py-1 text-xs transition-colors ${
                      scopeFilter === 'all' ? 'bg-background shadow-sm' : 'text-muted-foreground hover:text-foreground'
                    }`}
                  >
                    全部
                  </button>
                  <button
                    type="button"
                    onClick={() => handleScopeChange('global')}
                    className={`rounded px-2 py-1 text-xs transition-colors ${
                      scopeFilter === 'global' ? 'bg-background shadow-sm' : 'text-muted-foreground hover:text-foreground'
                    }`}
                  >
                    全局
                  </button>
                  <button
                    type="button"
                    onClick={() => handleScopeChange('local')}
                    className={`rounded px-2 py-1 text-xs transition-colors ${
                      scopeFilter === 'local' ? 'bg-background shadow-sm' : 'text-muted-foreground hover:text-foreground'
                    }`}
                  >
                    非全局
                  </button>
                </div>
              </div>
              <div className="min-h-0 flex-1 space-y-1 overflow-y-auto p-2">
                {scopeFilter === 'global' ? (
                  <div className="px-2 py-6 text-center text-sm text-muted-foreground">
                    全局黑话不按聊天划分
                  </div>
                ) : (
                  <>
                    <button
                      type="button"
                      onClick={() => handleChatChange('all')}
                      className={`w-full rounded-md px-2 py-2 text-left text-sm transition-colors ${
                        filterChatId === 'all'
                          ? 'bg-primary text-primary-foreground'
                          : 'text-foreground hover:bg-muted'
                      }`}
                    >
                      全部聊天
                    </button>
                    {chatList.map((chat) => (
                      <button
                        key={chat.session_id}
                        type="button"
                        onClick={() => handleChatChange(chat.session_id)}
                        className={`w-full rounded-md px-2 py-2 text-left text-sm transition-colors ${
                          filterChatId === chat.session_id
                            ? 'bg-primary text-primary-foreground'
                            : 'text-foreground hover:bg-muted'
                        }`}
                        title={chat.chat_name}
                      >
                        <span className="block truncate">{chat.chat_name}</span>
                      </button>
                    ))}
                  </>
                )}
              </div>
            </aside>

            <div className="min-h-0 lg:h-full">
              <JargonList
                jargons={jargons}
                loading={loading}
                total={total}
                page={page}
                pageSize={pageSize}
                selectedIds={selectedIds}
                hideChatColumn={scopeFilter === 'global' || filterChatId !== 'all'}
                className="lg:h-full"
                onEdit={handleEdit}
                onViewDetail={handleViewDetail}
                onDelete={(jargon) => setDeleteConfirmJargon(jargon)}
                onToggleSelect={toggleSelect}
                onToggleSelectAll={toggleSelectAll}
                onPageChange={setPage}
                onJumpToPage={handleJumpToPage}
              />
            </div>
          </div>
        </div>
      </ScrollArea>

      {/* 详情对话框 */}
      <JargonDetailDialog
        jargon={selectedJargon}
        open={isDetailDialogOpen}
        onOpenChange={setIsDetailDialogOpen}
      />

      {/* 创建对话框 */}
      <JargonCreateDialog
        open={isCreateDialogOpen}
        onOpenChange={setIsCreateDialogOpen}
        chatList={formChatList}
        onSuccess={() => {
          loadJargons()
          loadStats()
          loadChatList()
          setIsCreateDialogOpen(false)
        }}
      />

      {/* 编辑对话框 */}
      <JargonEditDialog
        jargon={selectedJargon}
        open={isEditDialogOpen}
        onOpenChange={setIsEditDialogOpen}
        chatList={formChatList}
        onSuccess={() => {
          loadJargons()
          loadStats()
          loadChatList()
          setIsEditDialogOpen(false)
        }}
      />

      {/* 删除确认对话框 */}
      <DeleteConfirmDialog
        jargon={deleteConfirmJargon}
        open={!!deleteConfirmJargon}
        onOpenChange={() => setDeleteConfirmJargon(null)}
        onConfirm={handleDelete}
      />

      {/* 批量删除确认对话框 */}
      <BatchDeleteConfirmDialog
        open={isBatchDeleteDialogOpen}
        onOpenChange={setIsBatchDeleteDialogOpen}
        onConfirm={handleBatchDelete}
        count={selectedIds.size}
      />
    </div>
  )
}
