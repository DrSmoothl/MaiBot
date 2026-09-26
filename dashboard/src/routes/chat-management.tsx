import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  Plus,
  RefreshCw,
  Search,
  Trash2,
  UserRound,
  UsersRound,
  X,
} from 'lucide-react'
import type { CSSProperties } from 'react'
import { useEffect, useMemo, useRef, useState } from 'react'

import { ChatStreamSettingsDialog } from '@/components/chat-stream-settings-dialog'
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { DashboardTabBar, DashboardTabTrigger } from '@/components/ui/dashboard-tabs'
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
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Tabs } from '@/components/ui/tabs'
import { useToast } from '@/hooks/use-toast'
import { useResolvedAvatarUrl } from '@/lib/avatar-url'
import {
  formatChatAccountLabel,
  formatChatDisplayName,
  getChatTypeText,
} from '@/lib/chat-display'
import { getChatStreams, type ChatStream, type ChatStreamType } from '@/lib/chat-management-api'
import { getBotConfig, updateBotConfigSection } from '@/lib/config-api'
import { cn } from '@/lib/utils'

const PAGE_SIZE = 10
type ChatTypeFilter = 'all' | ChatStreamType
type ChatManagementView = 'groups' | 'streams'
type MutualGroupKind = 'expression' | 'jargon' | 'memory'
const MUTUAL_GROUP_CHAT_RESULT_LIMIT = 50

function getRequestedSessionId(): string | null {
  if (typeof window === 'undefined') {
    return null
  }
  return new URLSearchParams(window.location.search).get('session_id')?.trim() || null
}

const MUTUAL_GROUP_KIND_LABEL: Record<MutualGroupKind, string> = {
  expression: '表达',
  jargon: '黑话',
  memory: '记忆',
}

interface TargetItem {
  platform: string
  item_id: string
  rule_type?: ChatStreamType | string
  type?: ChatStreamType | string
}

interface ChatStreamGroupConfig {
  targets?: TargetItem[]
  expression_groups?: TargetItem[]
  jargon_groups?: TargetItem[]
}

function formatTimestamp(timestamp: number | null): string {
  if (!timestamp) {
    return '-'
  }

  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(timestamp * 1000))
}

function getChatTypeLabel(chat: ChatStream): string {
  return chat.chat_type === 'group' ? '群聊' : '私聊'
}

function getChatLogicalId(chat: ChatStream): string {
  return chat.target_id || (chat.chat_type === 'group' ? chat.group_id : chat.user_id) || '-'
}

function getTargetRuleType(target: TargetItem): ChatStreamType {
  return target.rule_type === 'private' || target.type === 'private' ? 'private' : 'group'
}

function normalizeTarget(target: unknown): TargetItem | null {
  if (!target || typeof target !== 'object') {
    return null
  }
  const rawTarget = target as Record<string, unknown>
  const platform = String(rawTarget.platform ?? '').trim()
  const itemId = String(rawTarget.item_id ?? '').trim()
  const rawRuleType = rawTarget.rule_type ?? rawTarget.type
  const ruleType = rawRuleType === 'private' ? 'private' : 'group'
  if (!platform || !itemId) {
    return null
  }
  return { platform, item_id: itemId, rule_type: ruleType }
}

function normalizeMutualGroups(value: unknown): ChatStreamGroupConfig[] {
  if (!Array.isArray(value)) {
    return []
  }
  return value.map((group) => {
    if (!group || typeof group !== 'object') {
      return { targets: [] }
    }
    const rawGroup = group as ChatStreamGroupConfig
    const rawTargets =
      rawGroup.targets ?? rawGroup.expression_groups ?? rawGroup.jargon_groups ?? []
    const targets = Array.isArray(rawTargets)
      ? rawTargets.map(normalizeTarget).filter((target): target is TargetItem => target !== null)
      : []
    return { targets }
  })
}

function serializeMutualGroups(groups: ChatStreamGroupConfig[]): ChatStreamGroupConfig[] {
  return groups.map((group) => ({
    targets: (group.targets ?? []).map((target) => ({
      platform: target.platform,
      item_id: target.item_id,
      rule_type: getTargetRuleType(target),
    })),
  }))
}

function targetKey(target: TargetItem): string {
  return `${target.platform}:${target.item_id}:${getTargetRuleType(target)}`
}

function targetLabel(target: TargetItem): string {
  return `${target.platform}:${target.item_id}:${getChatTypeText(getTargetRuleType(target))}`
}

function getTargetDisplayName(
  target: TargetItem,
  chatNameByTargetKey: Map<string, string>
): string {
  return chatNameByTargetKey.get(targetKey(target)) ?? '未找到聊天流'
}

function chatToTarget(chat: ChatStream): TargetItem {
  return {
    platform: chat.platform,
    item_id: getChatLogicalId(chat),
    rule_type: chat.chat_type,
  }
}

function HoverScrollText({
  className,
  maxChars,
  value,
}: {
  className?: string
  maxChars: number
  value: string | null | undefined
}) {
  const text = value || '-'
  const containerRef = useRef<HTMLSpanElement>(null)
  const textRef = useRef<HTMLSpanElement>(null)
  const [shouldScroll, setShouldScroll] = useState(false)
  const [scrollDurationMs, setScrollDurationMs] = useState(900)

  useEffect(() => {
    const containerElement = containerRef.current
    const textElement = textRef.current
    if (!containerElement || !textElement) return

    const updateOverflowState = () => {
      const overflowWidth = textElement.scrollWidth - containerElement.clientWidth
      const nextShouldScroll = overflowWidth > 1
      setShouldScroll((current) => (current === nextShouldScroll ? current : nextShouldScroll))
      setScrollDurationMs(Math.max(900, Math.min(2800, overflowWidth * 36)))
    }

    updateOverflowState()

    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', updateOverflowState)
      return () => window.removeEventListener('resize', updateOverflowState)
    }

    const resizeObserver = new ResizeObserver(updateOverflowState)
    resizeObserver.observe(containerElement)
    resizeObserver.observe(textElement)
    return () => resizeObserver.disconnect()
  }, [maxChars, text])

  return (
    <span
      ref={containerRef}
      className={cn('group inline-block overflow-hidden align-bottom', className)}
      style={{ width: `${maxChars}ch` }}
      title={text}
    >
      <span
        ref={textRef}
        className={cn(
          'block max-w-full overflow-hidden text-ellipsis whitespace-nowrap',
          shouldScroll &&
            'group-hover:w-max group-hover:max-w-none group-hover:animate-[chat-management-text-scroll_1s_linear_infinite_alternate] group-hover:overflow-visible'
        )}
        style={
          {
            '--scroll-container-width': `${maxChars}ch`,
            animationDuration: `${scrollDurationMs}ms`,
          } as CSSProperties
        }
      >
        {text}
      </span>
    </span>
  )
}

function matchesSearch(chat: ChatStream, query: string): boolean {
  const normalizedQuery = query.trim().toLowerCase()
  if (!normalizedQuery) {
    return true
  }

  return [
    chat.id,
    chat.display_name,
    chat.session_id,
    chat.chat_type,
    chat.target_id,
    chat.platform,
    chat.account_id,
    chat.group_id,
    chat.group_name,
    chat.user_id,
    chat.user_nickname,
    chat.user_cardname,
  ]
    .filter(Boolean)
    .some((value) => String(value).toLowerCase().includes(normalizedQuery))
}

function matchesTypeFilter(chat: ChatStream, filter: ChatTypeFilter): boolean {
  return filter === 'all' || chat.chat_type === filter
}

function ChatStreamAvatar({ chat }: { chat: ChatStream }) {
  const targetType = chat.chat_type === 'group' ? 'group' : 'user'
  const targetId = chat.chat_type === 'group' ? chat.group_id : chat.user_id
  const avatarUrl = useResolvedAvatarUrl(chat.platform, targetId, targetType)
  const Icon = chat.chat_type === 'group' ? UsersRound : UserRound

  return (
    <Avatar className="border-border ring-background h-8 w-8 rounded-md border-2 ring-1">
      {avatarUrl && (
        <AvatarImage src={avatarUrl} alt={`${chat.display_name} 的头像`} className="object-cover" />
      )}
      <AvatarFallback className="text-muted-foreground rounded-md">
        <Icon className="h-4 w-4" />
      </AvatarFallback>
    </Avatar>
  )
}

function MutualGroupsView({ chats }: { chats: ChatStream[] }) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [kind, setKind] = useState<MutualGroupKind>(() => {
    if (typeof window === 'undefined') {
      return 'expression'
    }
    const queryKind = new URLSearchParams(window.location.search).get('kind')
    return queryKind === 'memory' || queryKind === 'jargon' ? queryKind : 'expression'
  })
  const [addDialogGroupIndex, setAddDialogGroupIndex] = useState<number | null>(null)
  const [addDialogSearch, setAddDialogSearch] = useState('')
  const [selectedTargetKeys, setSelectedTargetKeys] = useState<string[]>([])
  const configQuery = useQuery({
    queryKey: ['chat-management-mutual-groups-config'],
    queryFn: () => getBotConfig(),
  })
  const sectionName = kind === 'memory' ? 'a_memorix' : kind
  const groupFieldName =
    kind === 'memory'
      ? 'shared_memory_groups'
      : kind === 'expression'
        ? 'expression_groups'
        : 'jargon_groups'
  const sectionData = useMemo(() => {
    const raw = configQuery.data?.[sectionName]
    return (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>
  }, [configQuery.data, sectionName])
  const globalMemorySharingEnabled =
    kind === 'memory' && sectionData.global_memory_sharing_enabled === true
  const groups = useMemo(
    () => normalizeMutualGroups(sectionData[groupFieldName]),
    [groupFieldName, sectionData]
  )
  const addDialogGroup = addDialogGroupIndex === null ? null : (groups[addDialogGroupIndex] ?? null)
  const selectedTargetKeySet = useMemo(() => new Set(selectedTargetKeys), [selectedTargetKeys])
  const addDialogExistingKeySet = useMemo(
    () => new Set((addDialogGroup?.targets ?? []).map(targetKey)),
    [addDialogGroup]
  )
  const chatNameByTargetKey = useMemo(
    () =>
      new Map(
        chats.map((chat) => [
          targetKey(chatToTarget(chat)),
          formatChatDisplayName(chat.display_name, chat.account_id),
        ])
      ),
    [chats]
  )
  const addDialogChats = useMemo(() => {
    const keyword = addDialogSearch.trim().toLowerCase()
    return chats.filter((chat) => {
      const target = chatToTarget(chat)
      if (addDialogExistingKeySet.has(targetKey(target))) {
        return false
      }
      if (!keyword) {
        return true
      }
      return [
        chat.display_name,
        chat.account_id,
        chat.platform,
        getChatLogicalId(chat),
        chat.user_id,
        chat.group_id,
        chat.session_id,
        getChatTypeText(chat.chat_type),
      ]
        .join(' ')
        .toLowerCase()
        .includes(keyword)
    })
  }, [addDialogExistingKeySet, addDialogSearch, chats])
  const visibleAddDialogChats = addDialogChats.slice(0, MUTUAL_GROUP_CHAT_RESULT_LIMIT)
  const isAddDialogLimited = addDialogChats.length > visibleAddDialogChats.length

  const saveMutation = useMutation({
    mutationFn: (nextSectionData: Record<string, unknown>) =>
      updateBotConfigSection(sectionName, nextSectionData),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['chat-management-mutual-groups-config'] })
      toast({
        title: '共享组已保存',
        description: `${MUTUAL_GROUP_KIND_LABEL[kind]}共享组配置已更新。`,
      })
    },
    onError: (error) => {
      toast({
        title: '保存共享组失败',
        description: error instanceof Error ? error.message : '请稍后重试',
        variant: 'destructive',
      })
    },
  })

  const updateGroups = (nextGroups: ChatStreamGroupConfig[]) => {
    if (globalMemorySharingEnabled) {
      return
    }
    saveMutation.mutate({
      ...sectionData,
      [groupFieldName]: serializeMutualGroups(nextGroups),
    })
  }

  const createGroup = () => {
    if (globalMemorySharingEnabled) {
      return
    }
    updateGroups([...groups, { targets: [] }])
  }

  const openAddDialog = (groupIndex: number) => {
    if (globalMemorySharingEnabled) {
      return
    }
    setAddDialogGroupIndex(groupIndex)
    setAddDialogSearch('')
    setSelectedTargetKeys([])
  }

  const closeAddDialog = () => {
    setAddDialogGroupIndex(null)
    setAddDialogSearch('')
    setSelectedTargetKeys([])
  }

  const toggleAddDialogChat = (target: TargetItem) => {
    const key = targetKey(target)
    setSelectedTargetKeys((currentKeys) =>
      currentKeys.includes(key)
        ? currentKeys.filter((currentKey) => currentKey !== key)
        : [...currentKeys, key]
    )
  }

  const applySelectedChatsToGroup = () => {
    if (
      globalMemorySharingEnabled ||
      addDialogGroupIndex === null ||
      selectedTargetKeys.length === 0
    ) {
      return
    }
    const selectedKeySet = new Set(selectedTargetKeys)
    const selectedTargets = chats
      .map(chatToTarget)
      .filter((target) => selectedKeySet.has(targetKey(target)))
    const nextGroups = groups.map((group, index) => {
      if (index !== addDialogGroupIndex) {
        return group
      }
      const targets = group.targets ?? []
      const existingKeys = new Set(targets.map(targetKey))
      const nextTargets = selectedTargets.filter((target) => !existingKeys.has(targetKey(target)))
      return { targets: [...targets, ...nextTargets] }
    })
    updateGroups(nextGroups)
    closeAddDialog()
  }

  const removeTarget = (groupIndex: number, targetIndex: number) => {
    if (globalMemorySharingEnabled) {
      return
    }
    updateGroups(
      groups.map((group, index) =>
        index === groupIndex
          ? {
              targets: (group.targets ?? []).filter(
                (_, memberIndex) => memberIndex !== targetIndex
              ),
            }
          : group
      )
    )
  }

  const deleteGroup = (groupIndex: number) => {
    if (globalMemorySharingEnabled) {
      return
    }
    updateGroups(groups.filter((_, index) => index !== groupIndex))
  }
  const editingDisabled = saveMutation.isPending || globalMemorySharingEnabled

  return (
    <section className="bg-background flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border">
      <div className="flex shrink-0 flex-col gap-3 border-b p-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="space-y-1">
          <h2 className="text-base font-semibold">共享组管理</h2>
          <p className="text-muted-foreground text-sm">管理表达、黑话和记忆的聊天流共享组。</p>
        </div>
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <div className="bg-background inline-flex rounded-md border p-1">
            {[
              ['expression', '表达'],
              ['jargon', '黑话'],
              ['memory', '记忆'],
            ].map(([value, label]) => (
              <Button
                key={value}
                type="button"
                variant={kind === value ? 'secondary' : 'ghost'}
                size="sm"
                className="h-8"
                onClick={() => setKind(value as MutualGroupKind)}
              >
                {label}
              </Button>
            ))}
          </div>
          <Button type="button" disabled={editingDisabled} onClick={createGroup}>
            <Plus className="mr-2 h-4 w-4" />
            新建共享组
          </Button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-auto p-4">
        {globalMemorySharingEnabled && (
          <div className="bg-muted/30 text-muted-foreground mb-3 rounded-md border px-3 py-2 text-sm">
            全局共享记忆已开启，记忆共享组暂不参与普通记忆检索范围控制。
          </div>
        )}
        {configQuery.isLoading ? (
          <div className="space-y-3">
            <Skeleton className="h-20" />
            <Skeleton className="h-20" />
          </div>
        ) : configQuery.error ? (
          <div className="border-destructive/40 text-destructive rounded-md border p-4 text-sm">
            加载共享组失败
          </div>
        ) : groups.length === 0 ? (
          <div className="text-muted-foreground rounded-md border border-dashed p-6 text-center text-sm">
            暂无{MUTUAL_GROUP_KIND_LABEL[kind]}共享组。
          </div>
        ) : (
          <div className="grid gap-3">
            {groups.map((group, groupIndex) => (
              <div key={groupIndex} className="rounded-md border p-3">
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <div className="font-medium">共享组 {groupIndex + 1}</div>
                  <div className="flex items-center gap-2">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      disabled={editingDisabled}
                      onClick={() => openAddDialog(groupIndex)}
                    >
                      <Plus className="mr-2 h-4 w-4" />
                      添加聊天
                    </Button>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="text-destructive hover:text-destructive"
                      disabled={editingDisabled}
                      aria-label={`删除共享组 ${groupIndex + 1}`}
                      onClick={() => deleteGroup(groupIndex)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {(group.targets ?? []).length === 0 ? (
                    <span className="text-muted-foreground text-sm">空共享组</span>
                  ) : (
                    (group.targets ?? []).map((target, targetIndex) => (
                      <Badge
                        key={`${targetKey(target)}:${targetIndex}`}
                        variant="outline"
                        className="gap-2"
                        title={targetLabel(target)}
                      >
                        <span className="max-w-48 truncate text-xs">
                          {getTargetDisplayName(target, chatNameByTargetKey)}
                        </span>
                        <button
                          type="button"
                          className="text-muted-foreground hover:text-destructive"
                          disabled={editingDisabled}
                          aria-label={`移除 ${getTargetDisplayName(target, chatNameByTargetKey)}`}
                          onClick={() => removeTarget(groupIndex, targetIndex)}
                        >
                          <X className="h-3 w-3" />
                        </button>
                      </Badge>
                    ))
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <Dialog
        open={addDialogGroupIndex !== null}
        onOpenChange={(open) => !open && closeAddDialog()}
      >
        <DialogContent style={{ '--dialog-width': '42rem' } as CSSProperties}>
          <DialogHeader>
            <DialogTitle>添加聊天</DialogTitle>
            <DialogDescription>
              选择要加入共享组 {addDialogGroupIndex === null ? '' : addDialogGroupIndex + 1}{' '}
              的聊天流。
            </DialogDescription>
          </DialogHeader>
          <DialogBody>
            <div className="space-y-3">
              <Input
                value={addDialogSearch}
                onChange={(event) => setAddDialogSearch(event.target.value)}
                placeholder="搜索名称、平台、用户、群号或会话 ID"
              />
              <div className="max-h-[22rem] overflow-auto rounded-md border">
                {addDialogChats.length === 0 ? (
                  <div className="text-muted-foreground p-4 text-center text-sm">
                    没有可加入的聊天流
                  </div>
                ) : (
                  <div className="divide-y">
                    {visibleAddDialogChats.map((chat) => {
                      const target = chatToTarget(chat)
                      const key = targetKey(target)
                      const checked = selectedTargetKeySet.has(key)
                      return (
                        <label
                          key={chat.session_id}
                          className="hover:bg-muted/60 flex cursor-pointer items-center gap-3 px-3 py-2"
                        >
                          <Checkbox
                            checked={checked}
                            onCheckedChange={() => toggleAddDialogChat(target)}
                            aria-label={`选择 ${formatChatDisplayName(chat.display_name, chat.account_id)}`}
                          />
                          <div className="min-w-0 flex-1">
                            <div className="truncate text-sm font-medium">
                              {formatChatDisplayName(chat.display_name, chat.account_id)}
                            </div>
                            <div className="text-muted-foreground truncate font-mono text-xs">
                              {chat.platform}:{getChatLogicalId(chat)}
                            </div>
                          </div>
                          <Badge variant="outline">{getChatTypeText(chat.chat_type)}</Badge>
                        </label>
                      )
                    })}
                  </div>
                )}
              </div>
              {isAddDialogLimited && (
                <div className="text-muted-foreground text-xs">
                  仅显示前 {MUTUAL_GROUP_CHAT_RESULT_LIMIT} 个匹配项，请输入关键词缩小范围。
                </div>
              )}
            </div>
          </DialogBody>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={closeAddDialog}>
              取消
            </Button>
            <Button
              type="button"
              disabled={selectedTargetKeys.length === 0 || editingDisabled}
              onClick={applySelectedChatsToGroup}
            >
              加入 {selectedTargetKeys.length} 个聊天
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}

export function ChatManagementPage() {
  const requestedSessionId = useMemo(() => getRequestedSessionId(), [])
  const requestedSessionOpenedRef = useRef(false)
  const [activeView, setActiveView] = useState<ChatManagementView>(() => {
    if (typeof window === 'undefined') {
      return 'streams'
    }
    return new URLSearchParams(window.location.search).get('view') === 'groups'
      ? 'groups'
      : 'streams'
  })
  const [search, setSearch] = useState('')
  const [typeFilter, setTypeFilter] = useState<ChatTypeFilter>('all')
  const [page, setPage] = useState(1)
  const [selectedChat, setSelectedChat] = useState<ChatStream | null>(null)
  const {
    data: chats = [],
    error,
    isFetching,
    isLoading,
    refetch,
  } = useQuery({
    queryKey: ['chat-streams'],
    queryFn: () => getChatStreams(),
  })

  const filteredChats = useMemo(
    () =>
      chats.filter((chat) => matchesTypeFilter(chat, typeFilter) && matchesSearch(chat, search)),
    [chats, search, typeFilter]
  )
  const pageCount = Math.max(1, Math.ceil(filteredChats.length / PAGE_SIZE))
  const currentPage = Math.min(page, pageCount)
  const paginatedChats = useMemo(() => {
    const start = (currentPage - 1) * PAGE_SIZE
    return filteredChats.slice(start, start + PAGE_SIZE)
  }, [currentPage, filteredChats])
  const visibleStart = filteredChats.length === 0 ? 0 : (currentPage - 1) * PAGE_SIZE + 1
  const visibleEnd = Math.min(currentPage * PAGE_SIZE, filteredChats.length)
  const groupCount = chats.filter((chat) => chat.chat_type === 'group').length
  const privateCount = chats.length - groupCount

  useEffect(() => {
    if (!requestedSessionId || requestedSessionOpenedRef.current || chats.length === 0) {
      return
    }
    const requestedChat = chats.find((chat) => chat.session_id === requestedSessionId)
    if (requestedChat) {
      const frameId = window.requestAnimationFrame(() => {
        requestedSessionOpenedRef.current = true
        setSelectedChat(requestedChat)
      })
      return () => window.cancelAnimationFrame(frameId)
    }
  }, [chats, requestedSessionId])

  useEffect(() => {
    const frameId = window.requestAnimationFrame(() => setPage(1))
    return () => window.cancelAnimationFrame(frameId)
  }, [search, typeFilter])

  useEffect(() => {
    if (page > pageCount) {
      const frameId = window.requestAnimationFrame(() => setPage(pageCount))
      return () => window.cancelAnimationFrame(frameId)
    }
  }, [page, pageCount])

  const handleChatDeleted = (sessionId: string) => {
    if (selectedChat?.session_id === sessionId) {
      setSelectedChat(null)
    }
  }

  return (
    <main className="flex h-full min-h-0 flex-col gap-4 overflow-hidden p-4 md:p-6">
      <header className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="bg-background grid w-full grid-cols-3 border-2 text-sm sm:w-auto">
          <div className="px-4 py-2">
            <div className="text-muted-foreground">全部</div>
            <div className="text-lg leading-tight font-semibold">{chats.length}</div>
          </div>
          <div className="border-l-2 px-4 py-2">
            <div className="text-muted-foreground">群聊</div>
            <div className="text-lg leading-tight font-semibold">{groupCount}</div>
          </div>
          <div className="border-l-2 px-4 py-2">
            <div className="text-muted-foreground">私聊</div>
            <div className="text-lg leading-tight font-semibold">{privateCount}</div>
          </div>
        </div>
        <Tabs
          value={activeView}
          onValueChange={(value) => setActiveView(value as ChatManagementView)}
        >
          <DashboardTabBar className="bg-background h-10 w-full border-2 sm:w-fit">
            <DashboardTabTrigger value="streams" className="h-8 px-4">
              聊天流
            </DashboardTabTrigger>
            <DashboardTabTrigger value="groups" className="h-8 px-4">
              共享组
            </DashboardTabTrigger>
          </DashboardTabBar>
        </Tabs>
      </header>

      {activeView === 'groups' ? (
        <MutualGroupsView chats={chats} />
      ) : (
        <>
          <section className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="relative w-full sm:max-w-sm">
              <Search className="text-muted-foreground pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2" />
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="搜索名称、平台、用户、群号或会话 ID"
                className="pl-9"
              />
            </div>
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
              <Tabs
                value={typeFilter}
                onValueChange={(value) => setTypeFilter(value as ChatTypeFilter)}
              >
                <DashboardTabBar className="bg-background h-10 w-full border-2 sm:w-fit">
                  <DashboardTabTrigger value="all" className="h-8 px-4">
                    全部
                  </DashboardTabTrigger>
                  <DashboardTabTrigger value="group" className="h-8 px-4">
                    群聊
                  </DashboardTabTrigger>
                  <DashboardTabTrigger value="private" className="h-8 px-4">
                    私聊
                  </DashboardTabTrigger>
                </DashboardTabBar>
              </Tabs>
              <Button
                type="button"
                variant="outline"
                onClick={() => void refetch()}
                disabled={isFetching}
                className="shrink-0"
              >
                <RefreshCw className={cn('mr-2 h-4 w-4', isFetching && 'animate-spin')} />
                刷新
              </Button>
            </div>
          </section>

          <section className="bg-background flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border">
            <div className="min-h-0 flex-1 overflow-auto">
              <Table className="table-fixed">
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[7rem] px-3">聊天流</TableHead>
                    <TableHead className="w-[2rem] px-2">平台</TableHead>
                    <TableHead className="w-[5rem] px-2">ID</TableHead>
                    <TableHead className="w-[2.5rem] px-2">Type</TableHead>
                    <TableHead className="w-[3rem] px-2 text-right">消息数</TableHead>
                    <TableHead className="w-[3rem] px-2 text-right">表达数</TableHead>
                    <TableHead className="w-[3rem] px-2 text-right">黑话数</TableHead>
                    <TableHead className="w-[3rem] px-2">最后活跃</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {isLoading ? (
                    <TableRow>
                      <TableCell colSpan={8} className="text-muted-foreground h-28 text-center">
                        正在加载聊天流...
                      </TableCell>
                    </TableRow>
                  ) : error ? (
                    <TableRow>
                      <TableCell colSpan={8} className="text-destructive h-28 text-center">
                        加载聊天流失败
                      </TableCell>
                    </TableRow>
                  ) : filteredChats.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={8} className="text-muted-foreground h-28 text-center">
                        暂无匹配的聊天流
                      </TableCell>
                    </TableRow>
                  ) : (
                    paginatedChats.map((chat) => (
                      <TableRow
                        key={chat.session_id}
                        role="button"
                        tabIndex={0}
                        aria-label={`查看 ${formatChatDisplayName(chat.display_name, chat.account_id)} 详情`}
                        className="hover:bg-primary/10 focus-visible:bg-primary/10 focus-visible:outline-primary/60 cursor-pointer focus-visible:outline-2 focus-visible:outline-offset-[-2px]"
                        onClick={() => setSelectedChat(chat)}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault()
                            setSelectedChat(chat)
                          }
                        }}
                      >
                        <TableCell className="px-3">
                          <div className="flex min-w-0 items-center gap-3">
                            <ChatStreamAvatar chat={chat} />
                            <div className="min-w-0">
                              <HoverScrollText
                                className="font-medium"
                                maxChars={12}
                                value={chat.display_name}
                              />
                              {chat.account_id && (
                                <div className="text-muted-foreground truncate font-mono text-xs">
                                  {formatChatAccountLabel(chat.account_id)}
                                </div>
                              )}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell className="text-muted-foreground px-2 font-mono text-xs">
                          <HoverScrollText maxChars={4} value={chat.platform} />
                        </TableCell>
                        <TableCell className="text-muted-foreground px-2 font-mono text-xs">
                          <HoverScrollText maxChars={12} value={getChatLogicalId(chat)} />
                        </TableCell>
                        <TableCell className="px-2">
                          <Badge variant="outline">{getChatTypeLabel(chat)}</Badge>
                        </TableCell>
                        <TableCell className="px-2 text-right tabular-nums">
                          {chat.message_count}
                        </TableCell>
                        <TableCell className="px-2 text-right tabular-nums">
                          {chat.expression_count}
                        </TableCell>
                        <TableCell className="px-2 text-right tabular-nums">
                          {chat.jargon_count}
                        </TableCell>
                        <TableCell className="text-muted-foreground px-2">
                          {formatTimestamp(chat.last_active_at)}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
            <div className="text-muted-foreground flex shrink-0 flex-col gap-2 border-t px-3 py-2 text-xs sm:flex-row sm:items-center sm:justify-between">
              <div className="min-w-0">
                显示 {visibleStart}-{visibleEnd} / {filteredChats.length} 个聊天流
              </div>
              <div className="flex max-w-full min-w-0 items-center gap-1 overflow-x-auto sm:justify-end">
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0"
                  disabled={currentPage <= 1}
                  aria-label="第一页"
                  onClick={() => setPage(1)}
                >
                  <ChevronsLeft className="h-3.5 w-3.5" />
                </Button>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0"
                  disabled={currentPage <= 1}
                  aria-label="上一页"
                  onClick={() => setPage((value) => Math.max(1, value - 1))}
                >
                  <ChevronLeft className="h-3.5 w-3.5" />
                </Button>
                <span className="min-w-16 shrink-0 px-1 text-center tabular-nums">
                  {currentPage} / {pageCount}
                </span>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0"
                  disabled={currentPage >= pageCount}
                  aria-label="下一页"
                  onClick={() => setPage((value) => Math.min(pageCount, value + 1))}
                >
                  <ChevronRight className="h-3.5 w-3.5" />
                </Button>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0"
                  disabled={currentPage >= pageCount}
                  aria-label="最后一页"
                  onClick={() => setPage(pageCount)}
                >
                  <ChevronsRight className="h-3.5 w-3.5" />
                </Button>
              </div>
            </div>
          </section>
        </>
      )}

      <ChatStreamSettingsDialog
        chat={selectedChat}
        onOpenChange={(open) => !open && setSelectedChat(null)}
        onDeleted={handleChatDeleted}
      />
    </main>
  )
}
