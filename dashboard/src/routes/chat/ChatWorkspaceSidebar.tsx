import {
  Ban,
  Bot,
  Camera,
  Check,
  Edit2,
  Loader2,
  Search,
  Settings,
  UserCircle2,
  UserRound,
  UsersRound,
  X,
} from 'lucide-react'
import { useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useResolvedAvatarUrl } from '@/lib/avatar-url'
import type { SessionAdapterStatus } from '@/lib/chat-management-api'
import { cn } from '@/lib/utils'
import type { SessionInfo, StageStatusInfo } from '@/routes/monitor/use-maisaka-monitor'

import type { ChatMessage, ChatTab, ObservedMessagePreview } from './types'
import { getChatTabDisplayName } from './utils'

interface ChatWorkspaceSidebarProps {
  className?: string
  tabs: ChatTab[]
  activeTabId: string
  activeObservedSessionId: string | null
  observedSessions: Map<string, SessionInfo>
  observedStageStatuses: Map<string, StageStatusInfo>
  observedLatestMessages: Map<string, ObservedMessagePreview>
  observedAdapterStatuses: Map<string, SessionAdapterStatus>
  userId: string
  userName: string
  userAvatarVersion?: number
  isUploadingUserAvatar: boolean
  onSwitch: (tabId: string) => void
  onSelectObserved: (sessionId: string) => void
  onOpenObservedSettings: (sessionId: string) => void
  onClose: (tabId: string, e?: React.MouseEvent | React.KeyboardEvent) => void
  onUpdateUserAvatar: (file: File) => Promise<void>
  onUpdateUserName: (name: string) => void
}

function getMessagePreview(message: ChatMessage | undefined, fallback: string) {
  if (!message) return fallback
  if (message.type === 'system' || message.type === 'error') return message.content || fallback
  return message.content || fallback
}

function ConversationItem({
  tab,
  active,
  onSwitch,
  onClose,
}: {
  tab: ChatTab
  active: boolean
  onSwitch: (id: string) => void
  onClose: (id: string, e?: React.MouseEvent | React.KeyboardEvent) => void
}) {
  const { t } = useTranslation()
  const isVirtual = tab.type === 'virtual'
  const lastMessage = tab.messages[tab.messages.length - 1]
  const preview = getMessagePreview(lastMessage, t('chat.sidebar.emptyPreview'))
  const displayName = getChatTabDisplayName(tab, t('chat.botNameFallback'))
  const Icon = isVirtual ? UserCircle2 : Bot
  const avatarUrl = useResolvedAvatarUrl(
    isVirtual ? tab.virtualConfig?.platform : 'qq',
    isVirtual ? tab.virtualConfig?.userId : tab.sessionInfo.bot_qq
  )
  const avatarAlt = isVirtual
    ? `${tab.virtualConfig?.userName || tab.label} 的头像`
    : `${displayName} 的头像`

  return (
    <div
      className={cn(
        'group relative flex w-full min-w-0 items-center gap-1 rounded-xl pr-1 transition-colors',
        active
          ? 'bg-primary/12 text-foreground shadow-inner'
          : 'hover:bg-muted/70 text-foreground/90'
      )}
    >
      {active && (
        <span aria-hidden className="bg-primary absolute top-2 bottom-2 left-0 w-1 rounded-full" />
      )}
      <button
        type="button"
        className="flex w-full min-w-0 flex-1 items-center gap-2.5 overflow-hidden rounded-xl px-2.5 py-2 text-left"
        onClick={() => onSwitch(tab.id)}
      >
        <div className="relative shrink-0">
          <Avatar className="ring-border/60 h-9 w-9 ring-1">
            {avatarUrl && <AvatarImage src={avatarUrl} alt={avatarAlt} className="object-cover" />}
            <AvatarFallback
              className={cn(
                'text-xs',
                isVirtual
                  ? 'bg-secondary text-secondary-foreground'
                  : 'bg-primary-gradient text-primary-foreground'
              )}
            >
              <Icon className="h-5 w-5" />
            </AvatarFallback>
          </Avatar>
          <span
            aria-hidden
            className={cn(
              'border-card absolute right-0 bottom-0 h-3 w-3 rounded-full border-2 transition-colors',
              tab.isConnected ? 'bg-emerald-500' : 'bg-muted-foreground/40'
            )}
          />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 items-center justify-between gap-2">
            <span className="min-w-0 flex-1 truncate text-sm font-medium">{displayName}</span>
            {isVirtual && (
              <span className="bg-secondary text-secondary-foreground shrink-0 rounded-full px-1.5 py-0.5 text-[10px] font-medium tracking-wide">
                {t('chat.sidebar.virtualBadge')}
              </span>
            )}
          </div>
          <p className="text-muted-foreground mt-0.5 truncate text-xs">{preview}</p>
        </div>
      </button>

      {tab.id !== 'webui-default' && (
        <button
          type="button"
          aria-label={t('chat.sidebar.closeConversation', { label: displayName })}
          className="text-muted-foreground hover:bg-background hover:text-foreground rounded-md p-1 opacity-0 transition group-hover:opacity-100 focus-visible:opacity-100"
          onClick={(e) => onClose(tab.id, e)}
        >
          <X className="h-3.5 w-3.5" />
        </button>
      )}
    </div>
  )
}

function ObservedConversationItem({
  session,
  status,
  latestMessage,
  adapterStatus,
  active,
  onSelect,
  onOpenSettings,
}: {
  session: SessionInfo
  status?: StageStatusInfo
  latestMessage?: ObservedMessagePreview
  adapterStatus?: SessionAdapterStatus
  active: boolean
  onSelect: (sessionId: string) => void
  onOpenSettings: (sessionId: string) => void
}) {
  const { t } = useTranslation()
  const targetId = session.isGroupChat ? session.groupId : session.userId
  const targetType = session.isGroupChat ? 'group' : 'user'
  const avatarUrl = useResolvedAvatarUrl(session.platform, targetId ?? undefined, targetType)
  const Icon = session.isGroupChat ? UsersRound : UserRound
  // 最新消息预览：优先正文，纯媒体消息退回媒体占位文案
  const messagePreview =
    latestMessage?.content || latestMessage?.mediaText || t('chat.sidebar.emptyPreview')
  const previewText =
    session.isGroupChat && latestMessage?.speakerName
      ? `${latestMessage.speakerName}: ${messagePreview}`
      : messagePreview
  // 适配器不允许的聊天流实际不活跃，提示区分是被单独阻止还是名单未放行
  const adapterBlocked = adapterStatus?.allowed === false
  const adapterBlockedHint =
    adapterStatus?.reason === 'matched_deny_override'
      ? t('chat.sidebar.adapterBlockedOverrideHint')
      : t('chat.sidebar.adapterBlockedHint')

  return (
    <div
      className={cn(
        'group relative flex w-full min-w-0 items-center gap-1 rounded-xl pr-1 transition-colors',
        active
          ? 'bg-primary/12 text-foreground shadow-inner'
          : 'hover:bg-muted/70 text-foreground/90',
        adapterBlocked && 'opacity-70'
      )}
    >
      {active && (
        <span aria-hidden className="bg-primary absolute top-2 bottom-2 left-0 w-1 rounded-full" />
      )}
      <button
        type="button"
        className="flex w-full min-w-0 flex-1 items-center gap-2.5 overflow-hidden rounded-xl px-2.5 py-2 text-left"
        onClick={() => onSelect(session.sessionId)}
      >
        <div className="relative shrink-0">
          <Avatar className="ring-border/60 h-9 w-9 ring-1">
            {avatarUrl && (
              <AvatarImage
                src={avatarUrl}
                alt={t('chat.sidebar.observedAvatarAlt', { name: session.sessionName })}
                className="object-cover"
              />
            )}
            <AvatarFallback className="bg-secondary text-secondary-foreground">
              <Icon className="h-4.5 w-4.5" />
            </AvatarFallback>
          </Avatar>
          <span
            aria-hidden
            className={cn(
              'border-card absolute right-0 bottom-0 h-3 w-3 rounded-full border-2',
              status?.agentState === 'wait'
                ? 'bg-blue-500'
                : status
                  ? 'bg-emerald-500'
                  : 'bg-muted-foreground/40'
            )}
          />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 items-center gap-1">
            <span className="min-w-0 flex-1 truncate text-sm font-medium">{session.sessionName}</span>
            {adapterBlocked && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <span
                    aria-label={adapterBlockedHint}
                    className="text-muted-foreground shrink-0"
                    role="img"
                  >
                    <Ban className="h-3 w-3" />
                  </span>
                </TooltipTrigger>
                <TooltipContent side="right">{adapterBlockedHint}</TooltipContent>
              </Tooltip>
            )}
          </div>
          {status?.stage && (
            <p className="text-muted-foreground mt-0.5 truncate text-xs">{status.stage}</p>
          )}
          <p className="text-muted-foreground mt-0.5 truncate text-xs">{previewText}</p>
        </div>
      </button>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            aria-label={t('chat.sidebar.openSettings', { name: session.sessionName })}
            className="text-muted-foreground hover:bg-background hover:text-foreground rounded-md p-1 opacity-60 transition group-hover:opacity-100 focus-visible:opacity-100"
            onClick={() => onOpenSettings(session.sessionId)}
          >
            <Settings className="h-3.5 w-3.5" />
          </button>
        </TooltipTrigger>
        <TooltipContent side="right">
          {t('chat.sidebar.openSettings', { name: session.sessionName })}
        </TooltipContent>
      </Tooltip>
    </div>
  )
}

function ObservedConversationGroup({
  headingId,
  title,
  sessions,
  observedStageStatuses,
  observedLatestMessages,
  observedAdapterStatuses,
  activeObservedSessionId,
  onSelectObserved,
  onOpenObservedSettings,
}: {
  headingId: string
  title: string
  sessions: SessionInfo[]
  observedStageStatuses: Map<string, StageStatusInfo>
  observedLatestMessages: Map<string, ObservedMessagePreview>
  observedAdapterStatuses: Map<string, SessionAdapterStatus>
  activeObservedSessionId: string | null
  onSelectObserved: (sessionId: string) => void
  onOpenObservedSettings: (sessionId: string) => void
}) {
  if (sessions.length === 0) return null

  return (
    <div aria-labelledby={headingId} className="space-y-0.5" role="group">
      <h3
        id={headingId}
        className="text-muted-foreground flex items-center gap-1 px-2.5 pt-1.5 pb-0.5 text-[10px] font-medium tracking-wide"
      >
        {title}
        <span className="opacity-70">{sessions.length}</span>
      </h3>
      {sessions.map((session) => (
        <ObservedConversationItem
          key={session.sessionId}
          session={session}
          status={observedStageStatuses.get(session.sessionId)}
          latestMessage={observedLatestMessages.get(session.sessionId)}
          adapterStatus={observedAdapterStatuses.get(session.sessionId)}
          active={activeObservedSessionId === session.sessionId}
          onSelect={onSelectObserved}
          onOpenSettings={onOpenObservedSettings}
        />
      ))}
    </div>
  )
}

export function ChatWorkspaceSidebar({
  className,
  tabs,
  activeTabId,
  activeObservedSessionId,
  observedSessions,
  observedStageStatuses,
  observedLatestMessages,
  observedAdapterStatuses,
  userId,
  userName,
  userAvatarVersion,
  isUploadingUserAvatar,
  onSwitch,
  onSelectObserved,
  onOpenObservedSettings,
  onClose,
  onUpdateUserAvatar,
  onUpdateUserName,
}: ChatWorkspaceSidebarProps) {
  const { t } = useTranslation()
  const [editing, setEditing] = useState(false)
  const [draftName, setDraftName] = useState(userName)
  const [searchQuery, setSearchQuery] = useState('')
  const avatarInputRef = useRef<HTMLInputElement>(null)
  const nameInputAutofocusedRef = useRef(false)
  const userAvatarUrl = useResolvedAvatarUrl(
    userAvatarVersion ? 'webui' : undefined,
    userId,
    'user',
    userAvatarVersion
  )
  const sortedObservedSessions = Array.from(observedSessions.values()).sort(
    (a, b) => b.lastActivity - a.lastActivity
  )

  // 顶部搜索框：按显示名过滤本地会话与观察聊天流
  const normalizedQuery = searchQuery.trim().toLowerCase()
  const filteredTabs = normalizedQuery
    ? tabs.filter((tab) =>
        getChatTabDisplayName(tab, t('chat.botNameFallback')).toLowerCase().includes(normalizedQuery)
      )
    : tabs
  const filteredObservedSessions = normalizedQuery
    ? sortedObservedSessions.filter((session) =>
        session.sessionName.toLowerCase().includes(normalizedQuery)
      )
    : sortedObservedSessions
  // 适配器不允许的聊天流不再处理消息，单独归到不活跃分组
  const isAdapterBlocked = (sessionId: string) =>
    observedAdapterStatuses.get(sessionId)?.allowed === false
  const allowedObservedSessions = filteredObservedSessions.filter(
    (session) => !isAdapterBlocked(session.sessionId)
  )
  const blockedObservedSessions = filteredObservedSessions.filter((session) =>
    isAdapterBlocked(session.sessionId)
  )
  const showLocalSection = !normalizedQuery || filteredTabs.length > 0
  const showObservedSection = !normalizedQuery || filteredObservedSessions.length > 0
  const showNoResults =
    normalizedQuery.length > 0 && filteredTabs.length === 0 && filteredObservedSessions.length === 0

  const startEditing = () => {
    setDraftName(userName)
    nameInputAutofocusedRef.current = false
    setEditing(true)
  }

  const commit = () => {
    const next = draftName.trim() || t('chat.userNameFallback')
    onUpdateUserName(next)
    setEditing(false)
  }

  return (
    <aside
      className={cn(
        'bg-card/90 supports-backdrop-filter:bg-card/70 flex h-full shrink-0 flex-col border-r backdrop-blur',
        'w-60 xl:w-64',
        className
      )}
    >
      {/* 顶部搜索框 */}
      <div className="border-border border-b p-2">
        <div className="relative">
          <Search
            aria-hidden
            className="text-muted-foreground pointer-events-none absolute top-1/2 left-2.5 h-3.5 w-3.5 -translate-y-1/2"
          />
          <Input
            type="search"
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            placeholder={t('chat.sidebar.searchPlaceholder')}
            aria-label={t('chat.sidebar.searchPlaceholder')}
            className="h-8 pl-8 text-xs"
          />
        </div>
      </div>

      {/* 会话列表 */}
      <ScrollArea
        className="min-h-0 flex-1"
        contentClassName="!block w-full min-w-0"
        scrollbars="vertical"
        viewportClassName="[&>div]:!block [&>div]:!min-w-0 [&>div]:w-full"
      >
        <nav aria-label={t('chat.sidebar.conversations')} className="p-2">
          {showLocalSection && (
            <section aria-labelledby="chat-sidebar-local-heading" className="space-y-0.5">
              <h2
                id="chat-sidebar-local-heading"
                className="text-muted-foreground px-2.5 pt-0.5 pb-1 text-[11px] font-medium tracking-wide"
              >
                {t('chat.sidebar.myChats')}
              </h2>
              {filteredTabs.map((tab) => (
                <ConversationItem
                  key={tab.id}
                  active={activeObservedSessionId === null && activeTabId === tab.id}
                  tab={tab}
                  onSwitch={onSwitch}
                  onClose={onClose}
                />
              ))}
            </section>
          )}

          {showObservedSection && (
            <section
              aria-labelledby="chat-sidebar-observed-heading"
              className={cn('space-y-0.5 pt-2', showLocalSection && 'border-border mt-2 border-t')}
            >
              <h2
                id="chat-sidebar-observed-heading"
                className="text-muted-foreground px-2.5 pt-0.5 pb-1 text-[11px] font-medium tracking-wide"
              >
                {t('chat.sidebar.observedChats')}
              </h2>
              {filteredObservedSessions.length === 0 ? (
                <p className="text-muted-foreground px-2.5 py-2 text-xs">
                  {t('chat.sidebar.waitingObservedChats')}
                </p>
              ) : (
                <>
                  <ObservedConversationGroup
                    headingId="chat-sidebar-observed-allowed-heading"
                    title={t('chat.sidebar.adapterAllowedGroup')}
                    sessions={allowedObservedSessions}
                    observedStageStatuses={observedStageStatuses}
                    observedLatestMessages={observedLatestMessages}
                    observedAdapterStatuses={observedAdapterStatuses}
                    activeObservedSessionId={activeObservedSessionId}
                    onSelectObserved={onSelectObserved}
                    onOpenObservedSettings={onOpenObservedSettings}
                  />
                  <ObservedConversationGroup
                    headingId="chat-sidebar-observed-blocked-heading"
                    title={t('chat.sidebar.adapterBlockedGroup')}
                    sessions={blockedObservedSessions}
                    observedStageStatuses={observedStageStatuses}
                    observedLatestMessages={observedLatestMessages}
                    observedAdapterStatuses={observedAdapterStatuses}
                    activeObservedSessionId={activeObservedSessionId}
                    onSelectObserved={onSelectObserved}
                    onOpenObservedSettings={onOpenObservedSettings}
                  />
                </>
              )}
            </section>
          )}

          {showNoResults && (
            <p className="text-muted-foreground px-2.5 py-2 text-xs">
              {t('chat.sidebar.noSearchResults')}
            </p>
          )}
        </nav>
      </ScrollArea>

      {/* 底部：本地用户身份 */}
      <div className="border-t p-2">
        <div className="bg-background/70 hover:bg-background flex items-center gap-2 rounded-xl border p-1.5 transition-colors">
          <div className="relative shrink-0">
            <Avatar className="ring-border/60 h-8 w-8 ring-1">
              {userAvatarUrl && (
                <AvatarImage
                  src={userAvatarUrl}
                  alt={t('chat.sidebar.userAvatarAlt', { name: userName })}
                  className="object-cover"
                />
              )}
              <AvatarFallback className="bg-secondary text-secondary-foreground">
                <UserCircle2 className="h-4 w-4" />
              </AvatarFallback>
            </Avatar>
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  type="button"
                  aria-label={t('chat.sidebar.editAvatar')}
                  className="bg-primary text-primary-foreground hover:bg-primary/90 border-card absolute -right-1 -bottom-1 flex h-4 w-4 items-center justify-center rounded-full border-2 shadow-sm transition disabled:cursor-wait"
                  disabled={isUploadingUserAvatar}
                  onClick={() => avatarInputRef.current?.click()}
                >
                  {isUploadingUserAvatar ? (
                    <Loader2 className="h-2 w-2 animate-spin" />
                  ) : (
                    <Camera className="h-2 w-2" />
                  )}
                </button>
              </TooltipTrigger>
              <TooltipContent side="top">
                {isUploadingUserAvatar
                  ? t('chat.sidebar.savingAvatar')
                  : t('chat.sidebar.editAvatar')}
              </TooltipContent>
            </Tooltip>
            <input
              ref={avatarInputRef}
              type="file"
              accept="image/jpeg,image/png,image/webp,image/gif,image/bmp"
              className="hidden"
              onChange={(event) => {
                const file = event.currentTarget.files?.[0]
                event.currentTarget.value = ''
                if (file) {
                  void onUpdateUserAvatar(file)
                }
              }}
            />
          </div>
          <div className="min-w-0 flex-1">
            {editing ? (
              <div className="flex items-center gap-1">
                <Input
                  ref={(element) => {
                    if (element && !nameInputAutofocusedRef.current) {
                      nameInputAutofocusedRef.current = true
                      element.focus()
                    }
                  }}
                  className="h-6 text-xs"
                  placeholder={t('chat.identity.namePlaceholder')}
                  value={draftName}
                  onChange={(e) => setDraftName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.nativeEvent.isComposing) {
                      e.preventDefault()
                      commit()
                    } else if (e.key === 'Escape') {
                      setEditing(false)
                    }
                  }}
                />
                <Button
                  aria-label={t('chat.sidebar.saveName')}
                  className="h-6 w-6 shrink-0"
                  size="icon"
                  variant="ghost"
                  onClick={commit}
                >
                  <Check className="h-3 w-3" />
                </Button>
              </div>
            ) : (
              <div className="flex min-w-0 items-center gap-1">
                <p className="min-w-0 flex-1 truncate text-sm font-medium">{userName}</p>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      aria-label={t('chat.sidebar.editName')}
                      className="h-5 w-5 shrink-0 opacity-60 hover:opacity-100"
                      size="icon"
                      variant="ghost"
                      onClick={startEditing}
                    >
                      <Edit2 className="h-2.5 w-2.5" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent side="top">{t('chat.sidebar.editName')}</TooltipContent>
                </Tooltip>
              </div>
            )}
          </div>
        </div>
      </div>
    </aside>
  )
}
