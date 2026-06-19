import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  Info,
  Plus,
  RefreshCw,
  Search,
  Trash2,
  UserRound,
  UsersRound,
} from 'lucide-react'
import type { CSSProperties, PointerEvent, ReactNode } from 'react'
import { useEffect, useMemo, useRef, useState } from 'react'

import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
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
import { Label } from '@/components/ui/label'
import { Progress } from '@/components/ui/progress'
import { Slider } from '@/components/ui/slider'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { useResolvedAvatarUrl } from '@/lib/avatar-url'
import {
  deleteChatStream,
  deleteChatStreamTalkFrequency,
  getChatStreamDetail,
  getChatStreams,
  updateChatStreamTalkFrequency,
  type ChatConfigRule,
  type ChatLearningStatus,
  type ChatStream,
  type ChatStreamDeleteResult,
  type ChatTalkFrequencyRule,
  type ChatStreamDetail,
  type ChatStreamType,
} from '@/lib/chat-management-api'
import { useToast } from '@/hooks/use-toast'
import { cn } from '@/lib/utils'

const PAGE_SIZE = 10
type ChatTypeFilter = 'all' | ChatStreamType

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

function getChatTypeText(chatType: ChatStreamType): string {
  return chatType === 'group' ? '群聊' : '私聊'
}

function getChatLogicalId(chat: ChatStream): string {
  return chat.target_id || (chat.chat_type === 'group' ? chat.group_id : chat.user_id) || '-'
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
  const shouldScroll = text.length > maxChars

  return (
    <span
      className={cn('group inline-block overflow-hidden align-bottom', className)}
      style={{ width: `${maxChars}ch` }}
      title={text}
    >
      <span
        className={cn(
          'block max-w-full overflow-hidden text-ellipsis whitespace-nowrap',
          shouldScroll &&
            'group-hover:w-max group-hover:max-w-none group-hover:animate-[chat-management-text-scroll_2.8s_linear_infinite_alternate] group-hover:overflow-visible'
        )}
        style={{ '--scroll-container-width': `${maxChars}ch` } as CSSProperties}
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

function formatRuleTarget(rule: ChatConfigRule | null): string {
  if (!rule) {
    return '未命中显式规则，使用默认行为'
  }
  if (rule.is_default) {
    return '默认规则'
  }
  const platform = rule.platform || '*'
  const itemId = rule.item_id || '*'
  return `${platform}:${itemId}:${getChatTypeText(rule.type === 'private' ? 'private' : 'group')}`
}

function StatusBadge({ enabled }: { enabled: boolean }) {
  return (
    <Badge variant={enabled ? 'default' : 'outline'} className={enabled ? '' : 'text-muted-foreground'}>
      {enabled ? '开启' : '关闭'}
    </Badge>
  )
}

function ChatStreamAvatar({ chat }: { chat: ChatStream }) {
  const targetType = chat.chat_type === 'group' ? 'group' : 'user'
  const targetId = chat.chat_type === 'group' ? chat.group_id : chat.user_id
  const avatarUrl = useResolvedAvatarUrl(chat.platform, targetId, targetType)
  const Icon = chat.chat_type === 'group' ? UsersRound : UserRound

  return (
    <Avatar className="h-8 w-8 rounded-md border-2 border-border ring-1 ring-background">
      {avatarUrl && <AvatarImage src={avatarUrl} alt={`${chat.display_name} 的头像`} className="object-cover" />}
      <AvatarFallback className="rounded-md text-muted-foreground">
        <Icon className="h-4 w-4" />
      </AvatarFallback>
    </Avatar>
  )
}

function ConfigStatusRow({ title, status }: { title: string; status: ChatLearningStatus }) {
  return (
    <div className="grid gap-3 rounded-md border p-3 text-sm lg:grid-cols-[5rem_1fr_1fr_minmax(12rem,1.5fr)] lg:items-center">
      <div className="text-base font-medium">{title}</div>
      <div className="flex items-center justify-between gap-3 lg:justify-start">
        <span className="text-muted-foreground">使用</span>
        <StatusBadge enabled={status.use} />
      </div>
      <div className="flex items-center justify-between gap-3 lg:justify-start">
        <span className="text-muted-foreground">学习</span>
        <StatusBadge enabled={status.learn} />
      </div>
      <div className="min-w-0 text-xs text-muted-foreground">
        命中规则：<span className="break-all">{formatRuleTarget(status.matched_rule)}</span>
      </div>
    </div>
  )
}

type TalkFrequencyEditMode = 'input' | 'slider' | 'timeline'
type TimelineEdge = 'end' | 'start'

interface TimelineRange {
  end: number
  start: number
}

const DAY_MINUTES = 24 * 60
const TIMELINE_TICKS = [0, 3, 6, 9, 12, 15, 18, 21, 24]
const TIMELINE_DRAG_STEP_MINUTES = 5

function clampTalkFrequencyValue(value: number): number {
  if (!Number.isFinite(value)) {
    return 0
  }
  return Math.max(0, Math.min(1, value))
}

function parseTimelineMinute(value: string): number | null {
  const match = /^(\d{1,2}):(\d{1,2})$/.exec(value.trim())
  if (!match) {
    return null
  }
  const hour = Number(match[1])
  const minute = Number(match[2])
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) {
    return null
  }
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    return null
  }
  return hour * 60 + minute
}

function formatTimelineMinute(minute: number): string {
  const normalizedMinute = Math.max(0, Math.min(DAY_MINUTES - 1, Math.round(minute)))
  const hour = Math.floor(normalizedMinute / 60)
  const minuteInHour = normalizedMinute % 60
  return `${hour.toString().padStart(2, '0')}:${minuteInHour.toString().padStart(2, '0')}`
}

function parseTalkTimeRange(time: string): TimelineRange | null {
  const normalizedTime = time.trim()
  if (!normalizedTime || normalizedTime === '*') {
    return { start: 0, end: DAY_MINUTES - 1 }
  }
  const [startRaw, endRaw, extra] = normalizedTime.split('-')
  if (extra !== undefined) {
    return null
  }
  const start = startRaw ? parseTimelineMinute(startRaw) : null
  const end = endRaw ? parseTimelineMinute(endRaw) : null
  if (start === null || end === null) {
    return null
  }
  return { start, end }
}

function formatTalkTimeRange(range: TimelineRange): string {
  return `${formatTimelineMinute(range.start)}-${formatTimelineMinute(range.end)}`
}

function getTimelineSegments(range: TimelineRange): Array<{ left: number; width: number }> {
  if (range.start <= range.end) {
    return [
      {
        left: (range.start / DAY_MINUTES) * 100,
        width: ((range.end - range.start + 1) / DAY_MINUTES) * 100,
      },
    ]
  }
  return [
    {
      left: (range.start / DAY_MINUTES) * 100,
      width: ((DAY_MINUTES - range.start) / DAY_MINUTES) * 100,
    },
    {
      left: 0,
      width: ((range.end + 1) / DAY_MINUTES) * 100,
    },
  ]
}

function getTimelineMinuteFromClient(clientX: number, timelineElement: HTMLElement): number {
  const rect = timelineElement.getBoundingClientRect()
  const ratio = rect.width > 0 ? (clientX - rect.left) / rect.width : 0
  const rawMinute = Math.max(0, Math.min(DAY_MINUTES - 1, ratio * DAY_MINUTES))
  return Math.round(rawMinute / TIMELINE_DRAG_STEP_MINUTES) * TIMELINE_DRAG_STEP_MINUTES
}

function talkValueColor(value: number): string {
  if (value >= 0.75) {
    return 'bg-emerald-500'
  }
  if (value >= 0.45) {
    return 'bg-amber-500'
  }
  return 'bg-sky-500'
}

function getExactTalkRules(detail: ChatStreamDetail): ChatTalkFrequencyRule[] {
  return detail.talk_frequency.matched_rules.filter((rule) => {
    return (
      String(rule.platform || '').trim() === detail.platform &&
      String(rule.item_id || '').trim() === detail.target_id &&
      String(rule.type || '').trim() === detail.chat_type
    )
  })
}

function formatFrequencySummary(label: string): string {
  const numericValue = Number.parseFloat(label)
  if (!Number.isFinite(numericValue)) {
    return label
  }
  return numericValue.toFixed(2)
}

function FrequencySummaryItem({
  formatValue = true,
  label,
  value,
}: {
  formatValue?: boolean
  label: string
  value: string
}) {
  return (
    <div className="min-w-0 space-y-1 text-sm">
      <div className="text-muted-foreground">{label}</div>
      <div className="whitespace-nowrap font-mono font-semibold tabular-nums">
        {formatValue ? formatFrequencySummary(value) : value}
      </div>
    </div>
  )
}

function TalkFrequencyRuleStackItem({ rule }: { rule: ChatTalkFrequencyRule }) {
  const targetLabel = `${rule.platform || '*'}:${rule.item_id || '*'}:${rule.type || '-'}`
  const timeLabel = rule.time || '默认'
  const timePriority = rule.time_priority ?? 0

  return (
    <div
      className={cn(
        'rounded-md border px-3 py-2 text-sm',
        rule.is_effective
          ? 'border-primary bg-primary/10 text-foreground'
          : 'bg-muted text-muted-foreground'
      )}
    >
      <div className="flex flex-wrap items-center gap-2">
        {rule.is_effective && <Badge variant="default">生效中</Badge>}
        {!rule.is_effective && !rule.time_active && <Badge variant="outline">时间未命中</Badge>}
        <span className="font-mono text-xs">{targetLabel}</span>
        <span className="text-xs">
          优先级 {rule.target_priority}.{timePriority}
        </span>
      </div>
      <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1">
        <span>时间：{timeLabel}</span>
        <span>频率：{formatFrequencySummary(rule.value_label)}</span>
      </div>
    </div>
  )
}

function TalkFrequencyRuleEditor({
  detail,
  mode,
  rule,
}: {
  detail: ChatStreamDetail
  mode: TalkFrequencyEditMode
  rule?: ChatTalkFrequencyRule
}) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const isNewRule = !rule
  const [time, setTime] = useState(rule?.time ?? '*')
  const [value, setValue] = useState(() =>
    clampTalkFrequencyValue(rule?.value ?? detail.talk_frequency.effective_value)
  )

  useEffect(() => {
    const frameId = window.requestAnimationFrame(() => {
      setTime(rule?.time ?? '*')
      setValue(clampTalkFrequencyValue(rule?.value ?? detail.talk_frequency.effective_value))
    })

    return () => window.cancelAnimationFrame(frameId)
  }, [detail.session_id, detail.talk_frequency.effective_value, rule])

  const updateDetailCache = (updatedDetail: ChatStreamDetail) => {
    queryClient.setQueryData(['chat-stream-detail', detail.session_id], updatedDetail)
    void queryClient.invalidateQueries({ queryKey: ['chat-streams'] })
  }

  const saveMutation = useMutation({
    mutationFn: () =>
      updateChatStreamTalkFrequency(detail.session_id, {
        previous_time: rule?.time ?? null,
        time: time.trim(),
        value: clampTalkFrequencyValue(value),
      }),
    onSuccess: (updatedDetail) => {
      updateDetailCache(updatedDetail)
      toast({
        title: isNewRule ? '发言频率规则已新增' : '发言频率规则已保存',
        description: '已写入当前聊天流的精确动态频率规则。',
      })
    },
    onError: (error) => {
      toast({
        title: '保存发言频率失败',
        description: error instanceof Error ? error.message : '请稍后重试',
        variant: 'destructive',
      })
    },
  })

  const deleteMutation = useMutation({
    mutationFn: () => deleteChatStreamTalkFrequency(detail.session_id, rule?.time ?? ''),
    onSuccess: (updatedDetail) => {
      updateDetailCache(updatedDetail)
      toast({
        title: '发言频率规则已删除',
        description: '已删除当前聊天流的这条精确规则。',
      })
    },
    onError: (error) => {
      toast({
        title: '删除发言频率规则失败',
        description: error instanceof Error ? error.message : '请稍后重试',
        variant: 'destructive',
      })
    },
  })

  return (
    <div className="grid gap-3 rounded-md border bg-muted/25 p-3 sm:grid-cols-[minmax(8rem,12rem)_1fr_auto] sm:items-end">
      <div className="space-y-2">
        <Label className="text-xs">
          {isNewRule ? '新增时间段' : '时间段'}
        </Label>
        <Input
          value={time}
          placeholder="* 或 HH:MM-HH:MM"
          onChange={(event) => setTime(event.target.value)}
        />
      </div>
      <div className="space-y-2">
        <Label className="text-xs">发言频率</Label>
        {mode === 'slider' ? (
          <div className="flex items-center gap-3">
            <Slider
              value={[value]}
              min={0}
              max={1}
              step={0.01}
              onValueChange={(values) => setValue(clampTalkFrequencyValue(values[0] ?? 0))}
              data-dashboard-slider="config"
              data-dashboard-slider-value-format="fixed-2"
            />
            <span className="w-12 text-right font-mono text-sm tabular-nums">{value.toFixed(2)}</span>
          </div>
        ) : (
          <Input
            type="number"
            min={0}
            max={1}
            step={0.01}
            value={value}
            onChange={(event) => setValue(clampTalkFrequencyValue(Number(event.target.value)))}
          />
        )}
      </div>
      <div className="flex items-center justify-end gap-2">
        <Button
          type="button"
          className="shrink-0"
          disabled={saveMutation.isPending}
          onClick={() => saveMutation.mutate()}
        >
          {saveMutation.isPending ? '保存中...' : isNewRule ? '新增' : '保存'}
        </Button>
        {!isNewRule && (
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="shrink-0 text-destructive hover:text-destructive"
            disabled={deleteMutation.isPending}
            aria-label={`删除时间段 ${rule.time || '默认'} 的发言频率规则`}
            onClick={() => deleteMutation.mutate()}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        )}
      </div>
    </div>
  )
}

function TalkFrequencyTimelineRule({
  detail,
  rule,
}: {
  detail: ChatStreamDetail
  rule?: ChatTalkFrequencyRule
}) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const isNewRule = !rule
  const [time, setTime] = useState(rule?.time || '00:00-23:59')
  const [value, setValue] = useState(() =>
    clampTalkFrequencyValue(rule?.value ?? detail.talk_frequency.effective_value)
  )
  const draggingEdgeRef = useRef<TimelineEdge | null>(null)
  const range = parseTalkTimeRange(time)
  const draggableRange = range && time.trim() !== '' && time.trim() !== '*'
  const segments = range ? getTimelineSegments(range) : []

  useEffect(() => {
    const frameId = window.requestAnimationFrame(() => {
      setTime(rule?.time || '00:00-23:59')
      setValue(clampTalkFrequencyValue(rule?.value ?? detail.talk_frequency.effective_value))
    })

    return () => window.cancelAnimationFrame(frameId)
  }, [detail.session_id, detail.talk_frequency.effective_value, rule])

  const updateDetailCache = (updatedDetail: ChatStreamDetail) => {
    queryClient.setQueryData(['chat-stream-detail', detail.session_id], updatedDetail)
    void queryClient.invalidateQueries({ queryKey: ['chat-streams'] })
  }

  const saveMutation = useMutation({
    mutationFn: () =>
      updateChatStreamTalkFrequency(detail.session_id, {
        previous_time: rule?.time ?? null,
        time: time.trim(),
        value: clampTalkFrequencyValue(value),
      }),
    onSuccess: (updatedDetail) => {
      updateDetailCache(updatedDetail)
      toast({
        title: isNewRule ? '发言频率规则已新增' : '发言频率规则已保存',
        description: '已写入当前聊天流的精确动态频率规则。',
      })
    },
    onError: (error) => {
      toast({
        title: '保存发言频率失败',
        description: error instanceof Error ? error.message : '请稍后重试',
        variant: 'destructive',
      })
    },
  })

  const deleteMutation = useMutation({
    mutationFn: () => deleteChatStreamTalkFrequency(detail.session_id, rule?.time ?? ''),
    onSuccess: (updatedDetail) => {
      updateDetailCache(updatedDetail)
      toast({
        title: '发言频率规则已删除',
        description: '已删除当前聊天流的这条精确规则。',
      })
    },
    onError: (error) => {
      toast({
        title: '删除发言频率规则失败',
        description: error instanceof Error ? error.message : '请稍后重试',
        variant: 'destructive',
      })
    },
  })

  const updateTimeFromPointer = (event: PointerEvent<HTMLElement>, edge: TimelineEdge) => {
    if (!range) {
      return
    }
    const timelineElement = event.currentTarget.closest('[data-chat-talk-timeline-track]')
    if (!(timelineElement instanceof HTMLElement)) {
      return
    }
    const nextMinute = getTimelineMinuteFromClient(event.clientX, timelineElement)
    const nextRange =
      edge === 'start'
        ? { ...range, start: nextMinute }
        : { ...range, end: nextMinute }
    setTime(formatTalkTimeRange(nextRange))
  }

  const startDrag = (event: PointerEvent<HTMLElement>, edge: TimelineEdge) => {
    event.preventDefault()
    draggingEdgeRef.current = edge
    event.currentTarget.setPointerCapture(event.pointerId)
    updateTimeFromPointer(event, edge)
  }

  return (
    <div className="grid gap-3 rounded-md border bg-muted/25 p-3 lg:grid-cols-[7rem_minmax(18rem,1fr)_8rem_8rem] lg:items-center">
      <div className="min-w-0">
        <Input
          value={time}
          placeholder="HH:MM-HH:MM"
          onChange={(event) => setTime(event.target.value)}
        />
      </div>
      <div className="min-w-0">
        <div className="relative mb-1 h-4 px-1 text-[10px] text-muted-foreground">
          {TIMELINE_TICKS.map((hour) => (
            <span
              key={hour}
              className="absolute -translate-x-1/2"
              style={{ left: `${(hour / 24) * 100}%` }}
            >
              {hour.toString().padStart(2, '0')}
            </span>
          ))}
        </div>
        <div className="relative h-8 rounded-md border bg-background" data-chat-talk-timeline-track>
          {TIMELINE_TICKS.slice(1, -1).map((hour) => (
            <span
              key={hour}
              className="absolute top-0 h-full border-l border-dashed border-muted-foreground/20"
              style={{ left: `${(hour / 24) * 100}%` }}
            />
          ))}
          {segments.map((segment, index) => (
            <span
              key={index}
              className={cn(
                'absolute top-1/2 h-4 -translate-y-1/2 rounded-sm opacity-85',
                talkValueColor(value),
                !range && 'opacity-35'
              )}
              style={{ left: `${segment.left}%`, width: `${segment.width}%` }}
            />
          ))}
          {draggableRange && (
            <>
              <button
                type="button"
                className="absolute top-1/2 h-7 w-2 -translate-x-1/2 -translate-y-1/2 cursor-ew-resize rounded-sm border border-background bg-foreground/80"
                style={{ left: `${(range.start / DAY_MINUTES) * 100}%` }}
                aria-label="调整开始时间"
                onPointerDown={(event) => startDrag(event, 'start')}
                onPointerMove={(event) => {
                  if (draggingEdgeRef.current === 'start' && event.currentTarget.hasPointerCapture(event.pointerId)) {
                    updateTimeFromPointer(event, 'start')
                  }
                }}
                onPointerUp={(event) => {
                  draggingEdgeRef.current = null
                  event.currentTarget.releasePointerCapture(event.pointerId)
                }}
              />
              <button
                type="button"
                className="absolute top-1/2 h-7 w-2 -translate-x-1/2 -translate-y-1/2 cursor-ew-resize rounded-sm border border-background bg-foreground/80"
                style={{ left: `${((range.end + 1) / DAY_MINUTES) * 100}%` }}
                aria-label="调整结束时间"
                onPointerDown={(event) => startDrag(event, 'end')}
                onPointerMove={(event) => {
                  if (draggingEdgeRef.current === 'end' && event.currentTarget.hasPointerCapture(event.pointerId)) {
                    updateTimeFromPointer(event, 'end')
                  }
                }}
                onPointerUp={(event) => {
                  draggingEdgeRef.current = null
                  event.currentTarget.releasePointerCapture(event.pointerId)
                }}
              />
            </>
          )}
        </div>
      </div>
      <div className="flex items-center gap-2">
        <Slider
          value={[value]}
          min={0}
          max={1}
          step={0.01}
          onValueChange={(values) => setValue(clampTalkFrequencyValue(values[0] ?? 0))}
          data-dashboard-slider="config"
          data-dashboard-slider-value-format="fixed-2"
        />
        <span className="w-10 text-right font-mono text-xs tabular-nums">{value.toFixed(2)}</span>
      </div>
      <div className="flex justify-end gap-2">
        <Button
          type="button"
          size="sm"
          disabled={saveMutation.isPending}
          onClick={() => saveMutation.mutate()}
        >
          {saveMutation.isPending ? '保存中...' : isNewRule ? '新增' : '保存'}
        </Button>
        {!isNewRule && (
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="h-8 w-8 text-destructive hover:text-destructive"
            disabled={deleteMutation.isPending}
            aria-label={`删除时间段 ${rule.time || '默认'} 的发言频率规则`}
            onClick={() => deleteMutation.mutate()}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        )}
      </div>
    </div>
  )
}

function TalkFrequencyTimelineEditor({
  detail,
  exactRules,
}: {
  detail: ChatStreamDetail
  exactRules: ChatTalkFrequencyRule[]
}) {
  return (
    <div className="space-y-2">
      <div className="grid grid-cols-[7rem_minmax(18rem,1fr)_8rem_8rem] gap-3 px-3 text-[11px] text-muted-foreground">
        <div>时间段</div>
        <div>时间轴</div>
        <div>频率</div>
        <div className="text-right">操作</div>
      </div>
      {exactRules.length === 0 ? (
        <div className="rounded-md border border-dashed px-3 py-2 text-sm text-muted-foreground">
          当前聊天流还没有专属发言频率规则。
        </div>
      ) : (
        <div className="space-y-2">
          {exactRules.map((rule, index) => (
            <TalkFrequencyTimelineRule
              key={`${rule.time}:${index}`}
              detail={detail}
              rule={rule}
            />
          ))}
        </div>
      )}
      <div className="space-y-2">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Plus className="h-4 w-4" />
          新增规则
        </div>
        <TalkFrequencyTimelineRule detail={detail} />
      </div>
    </div>
  )
}

function TalkFrequencyEditor({ detail }: { detail: ChatStreamDetail }) {
  const exactRules = useMemo(() => getExactTalkRules(detail), [detail])
  const [mode, setMode] = useState<TalkFrequencyEditMode>('timeline')

  return (
    <div className="space-y-3 rounded-md border bg-muted/10 p-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="text-sm font-medium">当前聊天流规则</div>
          <div className="mt-1 text-xs text-muted-foreground">
            仅编辑 {detail.platform}:{detail.target_id}:{getChatTypeText(detail.chat_type)} 的精确规则。
          </div>
        </div>
        <div className="inline-flex shrink-0 rounded-md border bg-background p-1">
          <Button
            type="button"
            size="sm"
            variant={mode === 'timeline' ? 'secondary' : 'ghost'}
            className="h-7"
            onClick={() => setMode('timeline')}
          >
            时间轴
          </Button>
          <Button
            type="button"
            size="sm"
            variant={mode === 'slider' ? 'secondary' : 'ghost'}
            className="h-7"
            onClick={() => setMode('slider')}
          >
            滑块
          </Button>
          <Button
            type="button"
            size="sm"
            variant={mode === 'input' ? 'secondary' : 'ghost'}
            className="h-7"
            onClick={() => setMode('input')}
          >
            普通
          </Button>
        </div>
      </div>

      {mode === 'timeline' ? (
        <TalkFrequencyTimelineEditor detail={detail} exactRules={exactRules} />
      ) : (
        <div className="space-y-3">
          {exactRules.length === 0 ? (
            <div className="rounded-md border border-dashed px-3 py-2 text-sm text-muted-foreground">
              当前聊天流还没有专属发言频率规则。
            </div>
          ) : (
            <div className="space-y-2">
              {exactRules.map((rule, index) => (
                <TalkFrequencyRuleEditor
                  key={`${rule.time}:${index}`}
                  detail={detail}
                  mode={mode}
                  rule={rule}
                />
              ))}
            </div>
          )}
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-sm font-medium">
              <Plus className="h-4 w-4" />
              新增规则
            </div>
            <TalkFrequencyRuleEditor detail={detail} mode={mode} />
          </div>
        </div>
      )}
    </div>
  )
}

function TalkFrequencySection({ detail }: { detail: ChatStreamDetail }) {
  return (
    <section className="space-y-3 rounded-md border p-3">
      <div className="flex items-center justify-between gap-3">
        <div className="font-medium">发言频率规则</div>
        <StatusBadge enabled={detail.talk_frequency.enabled} />
      </div>
      <div className="grid gap-2 text-sm sm:grid-cols-3">
        <FrequencySummaryItem label="默认频率" value={detail.talk_frequency.base_value_label} />
        <FrequencySummaryItem label="当前生效" value={detail.talk_frequency.effective_value_label} />
        <FrequencySummaryItem formatValue={false} label="当前时间" value={detail.talk_frequency.current_time} />
      </div>
      <div className="space-y-2">
        {detail.talk_frequency.matched_rules.length === 0 ? (
          <div className="rounded-md bg-muted px-3 py-2 text-sm text-muted-foreground">
            没有可应用的动态发言频率规则，使用默认频率。
          </div>
        ) : (
          detail.talk_frequency.matched_rules.map((rule, index) => (
            <TalkFrequencyRuleStackItem
              key={`${rule.platform}:${rule.item_id}:${rule.time}:${index}`}
              rule={rule}
            />
          ))
        )}
      </div>
      <TalkFrequencyEditor detail={detail} />
    </section>
  )
}

function ConfigStatusRows({ detail }: { detail: ChatStreamDetail }) {
  const configRows = [
    { title: '表达', status: detail.expression, visible: true },
    { title: '黑话', status: detail.jargon, visible: true },
    {
      title: '行为',
      status: detail.behavior,
      visible: Boolean(detail.behavior && (detail.behavior.use || detail.behavior.learn)),
    },
  ]

  return (
    <section className="space-y-2">
      {configRows.map((row) =>
        row.visible && row.status ? (
          <ConfigStatusRow key={row.title} title={row.title} status={row.status} />
        ) : null
      )}
    </section>
  )
}

function CompactDetailItem({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="min-w-0 space-y-1">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="min-w-0 break-all text-sm font-medium">{value}</div>
    </div>
  )
}

function ChatDetailContent({
  detail,
  loading,
  error,
}: {
  detail: ChatStreamDetail | undefined
  loading: boolean
  error: unknown
}) {
  if (loading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-16" />
        <Skeleton className="h-24" />
        <Skeleton className="h-32" />
      </div>
    )
  }

  if (error || !detail) {
    return <div className="rounded-md border border-destructive/40 p-4 text-sm text-destructive">加载详情失败</div>
  }

  return (
    <div className="space-y-5">
      <section className="space-y-3 rounded-md border p-3">
        <CompactDetailItem
          label="Session ID"
          value={<span className="font-mono text-xs font-normal">{detail.session_id}</span>}
        />
        <div className="grid gap-3 sm:grid-cols-3">
          <CompactDetailItem label="Platform" value={detail.platform || '-'} />
          <CompactDetailItem label="Type" value={getChatTypeText(detail.chat_type)} />
          <CompactDetailItem label="ID" value={<span className="font-mono">{detail.target_id || '-'}</span>} />
        </div>
      </section>

      <TalkFrequencySection detail={detail} />
      <ConfigStatusRows detail={detail} />
    </div>
  )
}

function formatDeleteSummary(result: ChatStreamDeleteResult): string {
  const visibleItems = result.items.filter((item) => item.count > 0 || (item.unlinked ?? 0) > 0)
  if (visibleItems.length === 0) {
    return '未发现可清理的数据。'
  }

  return visibleItems
    .map((item) => {
      if (item.key === 'jargons') {
        return `${item.label} 删除 ${item.count} 条，解除关联 ${item.unlinked ?? 0} 条`
      }
      return `${item.label} ${item.count} 条`
    })
    .join('；')
}

function DeleteChatStreamDialog({
  chat,
  onDeleted,
  onOpenChange,
}: {
  chat: ChatStream | null
  onDeleted: (sessionId: string) => void
  onOpenChange: (open: boolean) => void
}) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [confirmText, setConfirmText] = useState('')
  const [progress, setProgress] = useState(0)
  const [stage, setStage] = useState('等待确认')
  const [deleteResult, setDeleteResult] = useState<ChatStreamDeleteResult | null>(null)
  const deleteMutation = useMutation({
    mutationFn: (sessionId: string) => deleteChatStream(sessionId),
  })
  const isDeleting = deleteMutation.isPending
  const canDelete =
    Boolean(chat?.session_id) && confirmText.trim() === chat?.session_id && !isDeleting && !deleteResult

  useEffect(() => {
    if (!chat) {
      const frameId = window.requestAnimationFrame(() => {
        setConfirmText('')
        setProgress(0)
        setStage('等待确认')
        setDeleteResult(null)
      })

      return () => window.cancelAnimationFrame(frameId)
    }
  }, [chat])

  const handleDelete = async () => {
    if (!chat || !canDelete) {
      return
    }

    setDeleteResult(null)
    setProgress(12)
    setStage('提交删除请求')
    try {
      setProgress(35)
      setStage('清理聊天流关联数据')
      const result = await deleteMutation.mutateAsync(chat.session_id)
      setProgress(82)
      setStage('刷新聊天流列表')
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['chat-streams'] }),
        queryClient.removeQueries({ queryKey: ['chat-stream-detail', chat.session_id] }),
      ])
      setProgress(100)
      setStage('删除完成')
      setDeleteResult(result)
      onDeleted(chat.session_id)
      toast({
        title: '聊天流已删除',
        description: formatDeleteSummary(result),
      })
    } catch (error) {
      setProgress(0)
      setStage('删除失败')
      toast({
        title: '删除聊天流失败',
        description: error instanceof Error ? error.message : '请稍后重试',
        variant: 'destructive',
      })
    }
  }

  return (
    <Dialog open={chat !== null} onOpenChange={(open) => !isDeleting && onOpenChange(open)}>
      <DialogContent style={{ '--dialog-width': '38rem' } as CSSProperties}>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-destructive">
            <AlertTriangle className="h-5 w-5" />
            严肃确认：删除聊天流
          </DialogTitle>
          <DialogDescription>
            此操作不可撤销。删除后会清理所有与该 session_id 直接相关的数据。
          </DialogDescription>
        </DialogHeader>
        <DialogBody>
          <div className="space-y-4">
            <div className="rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm">
              <div className="font-medium text-destructive">将被清理的数据包括：</div>
              <ul className="mt-2 list-disc space-y-1 pl-5 text-muted-foreground">
                <li>聊天流记录和该 session_id 下的所有消息。</li>
                <li>表达学习、黑话关联、工具调用记录、行为学习记录。</li>
                <li>消息统计、高频词等以该聊天流为归属的数据。</li>
              </ul>
            </div>

            <div className="grid gap-2 text-sm">
              <div className="grid gap-1 rounded-md border bg-muted/30 p-3">
                <span className="text-muted-foreground">聊天流</span>
                <span className="font-medium">{chat?.display_name || '-'}</span>
                <span className="break-all font-mono text-xs text-muted-foreground">
                  {chat?.session_id || '-'}
                </span>
              </div>
              <Label htmlFor="delete-chat-session-confirm">
                请输入完整 session_id 以确认删除
              </Label>
              <Input
                id="delete-chat-session-confirm"
                value={confirmText}
                disabled={isDeleting}
                onChange={(event) => setConfirmText(event.target.value)}
                placeholder={chat?.session_id}
                className="font-mono text-xs"
              />
            </div>

            {(isDeleting || progress > 0 || deleteResult) && (
              <div className="space-y-2 rounded-md border p-3">
                <div className="flex items-center justify-between gap-3 text-sm">
                  <span className="font-medium">{stage}</span>
                  <span className="font-mono text-xs tabular-nums text-muted-foreground">
                    {progress}%
                  </span>
                </div>
                <Progress value={progress} className="h-2" />
                {deleteResult && (
                  <p className="text-xs text-muted-foreground">
                    {formatDeleteSummary(deleteResult)}
                  </p>
                )}
              </div>
            )}
          </div>
        </DialogBody>
        <DialogFooter>
          <Button variant="outline" disabled={isDeleting} onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <Button variant="destructive" disabled={!canDelete} onClick={() => void handleDelete()}>
            {isDeleting ? '删除中...' : '永久删除'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export function ChatManagementPage() {
  const [search, setSearch] = useState('')
  const [typeFilter, setTypeFilter] = useState<ChatTypeFilter>('all')
  const [page, setPage] = useState(1)
  const [selectedChat, setSelectedChat] = useState<ChatStream | null>(null)
  const [deletingChat, setDeletingChat] = useState<ChatStream | null>(null)
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
  const detailQuery = useQuery({
    queryKey: ['chat-stream-detail', selectedChat?.session_id],
    queryFn: () => getChatStreamDetail(selectedChat?.session_id ?? ''),
    enabled: Boolean(selectedChat?.session_id),
  })

  const filteredChats = useMemo(
    () => chats.filter((chat) => matchesTypeFilter(chat, typeFilter) && matchesSearch(chat, search)),
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
    <main className="flex h-full min-h-0 flex-col gap-5 overflow-hidden p-4 md:p-6">
      <header className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="grid grid-cols-3 gap-3 text-sm">
          <div className="rounded-md border bg-background px-3 py-2">
            <div className="text-muted-foreground">全部</div>
            <div className="mt-1 text-lg font-semibold">{chats.length}</div>
          </div>
          <div className="rounded-md border bg-background px-3 py-2">
            <div className="text-muted-foreground">群聊</div>
            <div className="mt-1 text-lg font-semibold">{groupCount}</div>
          </div>
          <div className="rounded-md border bg-background px-3 py-2">
            <div className="text-muted-foreground">私聊</div>
            <div className="mt-1 text-lg font-semibold">{privateCount}</div>
          </div>
        </div>
      </header>

      <section className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="relative w-full sm:max-w-sm">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="搜索名称、平台、用户、群号或会话 ID"
            className="pl-9"
          />
        </div>
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <div className="inline-flex rounded-md border bg-background p-1">
            {[
              ['all', '全部'],
              ['group', '群聊'],
              ['private', '私聊'],
            ].map(([value, label]) => (
              <Button
                key={value}
                type="button"
                variant={typeFilter === value ? 'secondary' : 'ghost'}
                size="sm"
                className="h-8"
                onClick={() => setTypeFilter(value as ChatTypeFilter)}
              >
                {label}
              </Button>
            ))}
          </div>
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

      <section className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border bg-background">
        <div className="min-h-0 flex-1 overflow-auto">
          <Table className="table-fixed">
            <TableHeader>
              <TableRow>
                <TableHead className="w-[15rem] px-3">聊天流</TableHead>
                <TableHead className="w-[4.5rem] px-2">平台</TableHead>
                <TableHead className="w-[8.5rem] px-2">ID</TableHead>
                <TableHead className="w-[5rem] px-2">Type</TableHead>
                <TableHead className="w-[5.5rem] px-2 text-right">消息数</TableHead>
                <TableHead className="w-[7.5rem] px-2">最后活跃</TableHead>
                <TableHead className="w-[4rem] px-2 text-right">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-28 text-center text-muted-foreground">
                    正在加载聊天流...
                  </TableCell>
                </TableRow>
              ) : error ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-28 text-center text-destructive">
                    加载聊天流失败
                  </TableCell>
                </TableRow>
              ) : filteredChats.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-28 text-center text-muted-foreground">
                    暂无匹配的聊天流
                  </TableCell>
                </TableRow>
              ) : (
                paginatedChats.map((chat) => (
                  <TableRow key={chat.session_id}>
                    <TableCell className="px-3">
                      <div className="flex min-w-0 items-center gap-3">
                        <ChatStreamAvatar chat={chat} />
                        <div className="min-w-0">
                          <HoverScrollText className="font-medium" maxChars={12} value={chat.display_name} />
                        </div>
                      </div>
                    </TableCell>
                    <TableCell className="px-2 font-mono text-xs text-muted-foreground">
                      <HoverScrollText maxChars={4} value={chat.platform} />
                    </TableCell>
                    <TableCell className="px-2 font-mono text-xs text-muted-foreground">
                      <HoverScrollText maxChars={12} value={getChatLogicalId(chat)} />
                    </TableCell>
                    <TableCell className="px-2">
                      <Badge variant="outline">{getChatTypeLabel(chat)}</Badge>
                    </TableCell>
                    <TableCell className="px-2 text-right tabular-nums">{chat.message_count}</TableCell>
                    <TableCell className="px-2 text-muted-foreground">
                      {formatTimestamp(chat.last_active_at)}
                    </TableCell>
                    <TableCell className="px-2 text-right">
                      <div className="flex justify-end">
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon"
                          aria-label={`查看 ${chat.display_name} 详情`}
                          onClick={() => setSelectedChat(chat)}
                        >
                          <Info className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
        <div className="flex shrink-0 flex-col gap-3 border-t px-4 py-3 text-sm text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
          <div className="min-w-0">
            显示 {visibleStart}-{visibleEnd} / {filteredChats.length} 个聊天流
          </div>
          <div className="flex max-w-full min-w-0 items-center gap-1 overflow-x-auto pb-1 sm:justify-end">
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-8 w-8 shrink-0"
              disabled={currentPage <= 1}
              aria-label="第一页"
              onClick={() => setPage(1)}
            >
              <ChevronsLeft className="h-4 w-4" />
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-8 w-8 shrink-0"
              disabled={currentPage <= 1}
              aria-label="上一页"
              onClick={() => setPage((value) => Math.max(1, value - 1))}
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span className="min-w-20 shrink-0 px-2 text-center tabular-nums">
              {currentPage} / {pageCount}
            </span>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-8 w-8 shrink-0"
              disabled={currentPage >= pageCount}
              aria-label="下一页"
              onClick={() => setPage((value) => Math.min(pageCount, value + 1))}
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-8 w-8 shrink-0"
              disabled={currentPage >= pageCount}
              aria-label="最后一页"
              onClick={() => setPage(pageCount)}
            >
              <ChevronsRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </section>

      <Dialog open={selectedChat !== null} onOpenChange={(open) => !open && setSelectedChat(null)}>
        <DialogContent style={{ '--dialog-width': '44rem' } as CSSProperties}>
          <DialogHeader>
            <DialogTitle>{selectedChat?.display_name || '聊天流详情'}</DialogTitle>
          </DialogHeader>
          <DialogBody>
            <ChatDetailContent
              detail={detailQuery.data}
              loading={detailQuery.isLoading || detailQuery.isFetching}
              error={detailQuery.error}
            />
          </DialogBody>
          <DialogFooter className="border-t pt-4">
            <Button
              type="button"
              variant="destructive"
              disabled={!selectedChat}
              onClick={() => {
                if (selectedChat) {
                  setDeletingChat(selectedChat)
                }
              }}
            >
              <Trash2 className="mr-2 h-4 w-4" />
              删除聊天流
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <DeleteChatStreamDialog
        chat={deletingChat}
        onDeleted={handleChatDeleted}
        onOpenChange={(open) => !open && setDeletingChat(null)}
      />
    </main>
  )
}
