import { useRef, useState, type PointerEvent } from 'react'

import { ChevronDown, ChevronUp, GripVertical, Plus, Trash2 } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Slider } from '@/components/ui/slider'
import { Textarea } from '@/components/ui/textarea'
import { fieldTitleClassName } from '@/components/dynamic-form/fieldStyle'
import type { FieldHookComponent } from '@/lib/field-hooks'
import type { ConfigSchema } from '@/types/config-schema'

import { createJsonFieldHook } from './JsonFieldHookFactory'
import { createListItemEditorHook } from './ListItemEditorHookFactory'

type ExpressionRuleType = 'group' | 'private'

interface ExpressionGroupTarget {
  platform: string
  item_id: string
  rule_type: ExpressionRuleType
}

interface ExpressionGroupValue {
  targets: ExpressionGroupTarget[]
}

interface PlatformAccountRow {
  platform: string
  account: string
}

const PLATFORM_ACCOUNT_ROW_GRID_CLASS =
  'grid gap-2 rounded-md border bg-muted/20 p-3 sm:grid-cols-[minmax(0,5.5rem)_minmax(0,8.5rem)_2.5rem] md:grid-cols-[minmax(0,6rem)_minmax(0,9.5rem)_2.5rem]'

const LEARNING_ITEM_FALLBACK_SCHEMA: ConfigSchema = {
  className: 'LearningItem',
  classDoc: '学习规则',
  fields: [
    {
      name: 'platform',
      type: 'string',
      label: {
        zh_CN: '平台',
        en_US: 'Platform',
        ja_JP: 'プラットフォーム',
      },
      description: '平台，与 ID 一起留空表示全局。',
      required: false,
      default: '',
      'x-widget': 'input',
      'x-icon': 'wifi',
    },
    {
      name: 'item_id',
      type: 'string',
      label: {
        zh_CN: '聊天流 ID',
        en_US: 'Chat stream ID',
        ja_JP: 'チャットストリーム ID',
      },
      description: '要单独配置的群号或用户 ID；留空表示默认规则。',
      required: false,
      default: '',
      'x-widget': 'input',
      'x-icon': 'hash',
    },
    {
      name: 'type',
      type: 'select',
      label: {
        zh_CN: '聊天类型',
        en_US: 'Chat type',
        ja_JP: 'チャット種別',
      },
      description: '这条规则作用于群聊还是私聊。',
      required: false,
      default: 'group',
      options: ['group', 'private'],
      'x-widget': 'select',
      'x-icon': 'users',
      'x-option-descriptions': {
        group: '群聊',
        private: '私聊',
      },
    },
    {
      name: 'use',
      type: 'boolean',
      label: {
        zh_CN: '使用',
        en_US: 'Use',
        ja_JP: '使用',
      },
      description: '是否在这个聊天里使用已学到的内容。',
      required: false,
      default: true,
      'x-widget': 'switch',
      'x-icon': 'message-square',
    },
    {
      name: 'learn',
      type: 'boolean',
      label: {
        zh_CN: '学习',
        en_US: 'Learn',
        ja_JP: '学習',
      },
      description: '是否从这个聊天里继续学习新内容。',
      required: false,
      default: true,
      'x-widget': 'switch',
      'x-icon': 'graduation-cap',
    },
  ],
}

interface TimelineSegment {
  left: number
  width: number
}

interface TalkRuleTimelineItem {
  groupKey: string
  groupLabel: string
  itemId: string
  platform: string
  ruleType: string
  index: number
  rawTime: string
  scopeLabel: string
  title: string
  timeLabel: string
  value: number
  invalidTime: boolean
  range: TimelineRange | null
  segments: TimelineSegment[]
}

interface TalkRuleTimelineGroup {
  key: string
  label: string
  itemId: string
  platform: string
  ruleType: string
  scopeLabel: string
  hasFallback: boolean
  hasWildcard: boolean
  items: TalkRuleTimelineItem[]
}

interface TimelineRange {
  end: number
  start: number
}

const DAY_MINUTES = 24 * 60
const TIMELINE_TICKS = [0, 3, 6, 9, 12, 15, 18, 21, 24]
const TIMELINE_DRAG_STEP_MINUTES = 5

const ruleTypeLabel = (rule: unknown) => {
  if (rule === 'private') return '私聊'
  if (rule === 'group') return '群聊'
  return rule ? String(rule) : '未指定'
}

const platformLabel = (item: Record<string, unknown>) => {
  const platform = typeof item.platform === 'string' ? item.platform.trim() : ''
  const itemId = typeof item.item_id === 'string' ? item.item_id.trim() : ''
  if (!platform && !itemId) return '全局'
  if (!platform) return itemId
  if (!itemId) return platform
  return `${platform}:${itemId}`
}

const talkTargetScopeLabel = (item: Record<string, unknown>) => {
  const platform = typeof item.platform === 'string' ? item.platform.trim() : ''
  const itemId = typeof item.item_id === 'string' ? item.item_id.trim() : ''
  if (!platform && !itemId) return '全局'
  if (platform === '*' || itemId === '*') return '通配'
  if (!platform || !itemId) return '默认'
  return '精确'
}

const talkRuleTargetValues = (item: Record<string, unknown>) => {
  const platform = typeof item.platform === 'string' ? item.platform.trim() : ''
  const itemId = typeof item.item_id === 'string' ? item.item_id.trim() : ''
  const ruleType = typeof item.rule_type === 'string' ? item.rule_type.trim() : 'group'
  return { platform, itemId, ruleType }
}

const talkRuleGroupKey = (item: Record<string, unknown>) => {
  const { platform, itemId, ruleType } = talkRuleTargetValues(item)
  return `${platform}\u0000${itemId}\u0000${ruleType}`
}

const talkRuleGroupLabel = (item: Record<string, unknown>) => {
  return `${platformLabel(item)} · ${ruleTypeLabel(talkRuleTargetValues(item).ruleType)}`
}

const parseTimelineMinute = (value: string) => {
  const match = /^(\d{1,2}):(\d{1,2})$/.exec(value.trim())
  if (!match) return null

  const hour = Number(match[1])
  const minute = Number(match[2])
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return null
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null

  return hour * 60 + minute
}

const formatTimelineMinute = (minute: number) => {
  const normalizedMinute = Math.max(0, Math.min(DAY_MINUTES - 1, Math.round(minute)))
  const hour = Math.floor(normalizedMinute / 60)
  const minuteInHour = normalizedMinute % 60
  return `${hour.toString().padStart(2, '0')}:${minuteInHour.toString().padStart(2, '0')}`
}

const parseTalkTimeRange = (time: string): TimelineRange | null => {
  const [startRaw, endRaw, extra] = time.trim().split('-')
  const start = startRaw ? parseTimelineMinute(startRaw) : null
  const end = endRaw ? parseTimelineMinute(endRaw) : null
  if (extra !== undefined || start === null || end === null) {
    return null
  }
  return { start, end }
}

const formatTalkTimeRange = (range: TimelineRange) => {
  return `${formatTimelineMinute(range.start)}-${formatTimelineMinute(range.end)}`
}

const getTimelineMinuteFromClient = (clientX: number, timelineElement: HTMLElement) => {
  const rect = timelineElement.getBoundingClientRect()
  const relativeX = Math.max(0, Math.min(rect.width, clientX - rect.left))
  const rawMinute = (relativeX / rect.width) * (DAY_MINUTES - 1)
  return Math.round(rawMinute / TIMELINE_DRAG_STEP_MINUTES) * TIMELINE_DRAG_STEP_MINUTES
}

const fullDaySegment = (): TimelineSegment => ({ left: 0, width: 100 })

const segmentFromMinutes = (start: number, end: number): TimelineSegment => ({
  left: (start / DAY_MINUTES) * 100,
  width: Math.max(((end - start) / DAY_MINUTES) * 100, 0.18),
})

const parseTalkTimeSegments = (time: string): { invalid: boolean; label: string; segments: TimelineSegment[] } => {
  const normalizedTime = time.trim()
  if (!normalizedTime) {
    return { invalid: false, label: '兜底', segments: [fullDaySegment()] }
  }
  if (normalizedTime === '*') {
    return { invalid: false, label: '强制全天', segments: [fullDaySegment()] }
  }

  const range = parseTalkTimeRange(normalizedTime)
  if (!range) {
    return { invalid: true, label: '时间格式错误', segments: [fullDaySegment()] }
  }
  const { end, start } = range

  if (start <= end) {
    return { invalid: false, label: normalizedTime, segments: [segmentFromMinutes(start, end + 1)] }
  }

  return {
    invalid: false,
    label: `${normalizedTime} 跨夜`,
    segments: [segmentFromMinutes(start, DAY_MINUTES), segmentFromMinutes(0, end + 1)],
  }
}

const normalizeTalkValue = (value: unknown) => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.max(0, Math.min(1, value))
  }
  if (typeof value === 'string') {
    const parsedValue = Number(value)
    if (Number.isFinite(parsedValue)) {
      return Math.max(0, Math.min(1, parsedValue))
    }
  }
  return 0
}

const talkValueColor = (value: number) => {
  if (value <= 0.25) return 'bg-sky-500'
  if (value <= 0.5) return 'bg-emerald-500'
  if (value <= 0.75) return 'bg-amber-500'
  return 'bg-rose-500'
}

const buildTalkTimelineItems = (items: Record<string, unknown>[]): TalkRuleTimelineItem[] => {
  return items.map((item, index) => {
    const time = typeof item.time === 'string' ? item.time : ''
    const parsedTime = parseTalkTimeSegments(time)
    const range = time.trim() && time.trim() !== '*' ? parseTalkTimeRange(time) : null
    const value = normalizeTalkValue(item.value)
    const scopeLabel = talkTargetScopeLabel(item)
    const { platform, itemId, ruleType } = talkRuleTargetValues(item)
    return {
      groupKey: talkRuleGroupKey(item),
      groupLabel: talkRuleGroupLabel(item),
      itemId,
      platform,
      ruleType,
      index,
      rawTime: time.trim(),
      scopeLabel,
      title: `轨道 ${index + 1}`,
      timeLabel: parsedTime.label,
      value,
      invalidTime: parsedTime.invalid,
      range,
      segments: parsedTime.segments,
    }
  })
}

const groupTalkTimelineItems = (items: TalkRuleTimelineItem[]): TalkRuleTimelineGroup[] => {
  const groups: TalkRuleTimelineGroup[] = []
  const groupMap = new Map<string, TalkRuleTimelineGroup>()

  for (const item of items) {
    const existingGroup = groupMap.get(item.groupKey)
    if (existingGroup) {
      existingGroup.items.push(item)
      continue
    }

    const nextGroup: TalkRuleTimelineGroup = {
      key: item.groupKey,
      label: item.groupLabel,
      itemId: item.itemId,
      platform: item.platform,
      ruleType: item.ruleType,
      scopeLabel: item.scopeLabel,
      hasFallback: item.rawTime === '',
      hasWildcard: item.rawTime === '*',
      items: [item],
    }
    groups.push(nextGroup)
    groupMap.set(item.groupKey, nextGroup)
  }

  for (const group of groups) {
    group.hasFallback = group.items.some((item) => item.rawTime === '')
    group.hasWildcard = group.items.some((item) => item.rawTime === '*')
  }

  return groups.map((group) => ({
    ...group,
    items: [...group.items].sort((a, b) => {
      const priority = (item: TalkRuleTimelineItem) => {
        if (item.rawTime === '') return 3
        if (item.rawTime === '*') return 1
        return 2
      }
      const priorityDiff = priority(a) - priority(b)
      return priorityDiff || a.index - b.index
    }),
  }))
}

const createTalkRuleForGroup = (
  group: Pick<TalkRuleTimelineGroup, 'itemId' | 'platform' | 'ruleType'>,
  time: string,
) => ({
  platform: group.platform,
  item_id: group.itemId,
  rule_type: group.ruleType || 'group',
  time,
  value: 0.5,
})

const normalizeTalkRuleItems = (
  items: Record<string, unknown>[],
  context?: { addedIndex?: number; changedIndex?: number },
) => {
  const preferredIndex = context?.changedIndex ?? context?.addedIndex
  const specialTimeOwner = new Map<string, number>()
  const duplicateIndexes = new Set<number>()

  items.forEach((item, index) => {
    const rawTime = typeof item.time === 'string' ? item.time.trim() : ''
    if (rawTime !== '' && rawTime !== '*') {
      return
    }

    const ownerKey = `${talkRuleGroupKey(item)}\u0000${rawTime}`
    const existingOwner = specialTimeOwner.get(ownerKey)
    if (existingOwner === undefined) {
      specialTimeOwner.set(ownerKey, index)
      return
    }

    if (index === preferredIndex) {
      duplicateIndexes.add(existingOwner)
      specialTimeOwner.set(ownerKey, index)
      return
    }

    duplicateIndexes.add(index)
  })

  if (duplicateIndexes.size === 0) {
    return items
  }

  return items.map((item, index) =>
    duplicateIndexes.has(index)
      ? {
          ...item,
          time: '00:00-23:59',
        }
      : item,
  )
}

const moveTalkRuleItem = (items: Record<string, unknown>[], fromIndex: number, toIndex: number) => {
  if (fromIndex === toIndex || fromIndex < 0 || toIndex < 0) {
    return items
  }
  if (fromIndex >= items.length || toIndex >= items.length) {
    return items
  }

  const nextItems = [...items]
  const [movedItem] = nextItems.splice(fromIndex, 1)
  nextItems.splice(toIndex, 0, movedItem)
  return nextItems
}

function TalkValueTimelineOverview({
  items,
  onAddItem,
  onItemFieldChange,
  onItemsChange,
  onRemoveItem,
}: {
  items: Record<string, unknown>[]
  onAddItem: (item?: Record<string, unknown>) => void
  onItemFieldChange: (index: number, fieldName: string, fieldValue: unknown) => void
  onItemsChange: (
    items: Record<string, unknown>[],
    context?: { addedIndex?: number; changedIndex?: number },
  ) => void
  onRemoveItem: (index: number) => void
}) {
  const timelineItems = buildTalkTimelineItems(items)
  const timelineGroups = groupTalkTimelineItems(timelineItems)
  const dragFrameRef = useRef<number | null>(null)
  const draggingTrackRef = useRef<{ groupKey: string; index: number } | null>(null)
  const lastDragTimeRef = useRef<string | null>(null)
  const pendingDragRef = useRef<{
    clientX: number
    edge: 'end' | 'start'
    item: TalkRuleTimelineItem
    timelineElement: HTMLElement
  } | null>(null)
  if (timelineItems.length === 0) {
    return null
  }

  const commitRangeUpdate = (
    item: TalkRuleTimelineItem,
    edge: 'end' | 'start',
    timelineElement: HTMLElement,
    clientX: number,
  ) => {
    if (!item.range) {
      return
    }

    const nextMinute = getTimelineMinuteFromClient(clientX, timelineElement)
    const nextRange =
      edge === 'start'
        ? { ...item.range, start: nextMinute }
        : { ...item.range, end: nextMinute }
    const nextTime = formatTalkTimeRange(nextRange)
    if (lastDragTimeRef.current === nextTime) {
      return
    }

    lastDragTimeRef.current = nextTime
    onItemFieldChange(item.index, 'time', nextTime)
  }

  const scheduleRangeUpdate = (
    event: PointerEvent<HTMLElement>,
    item: TalkRuleTimelineItem,
    edge: 'end' | 'start',
  ) => {
    const timelineElement = event.currentTarget.closest('[data-talk-timeline-track]')
    if (!(timelineElement instanceof HTMLElement)) {
      return
    }

    pendingDragRef.current = {
      clientX: event.clientX,
      edge,
      item,
      timelineElement,
    }

    if (dragFrameRef.current !== null) {
      return
    }

    dragFrameRef.current = window.requestAnimationFrame(() => {
      dragFrameRef.current = null
      const pendingDrag = pendingDragRef.current
      if (!pendingDrag) {
        return
      }
      commitRangeUpdate(
        pendingDrag.item,
        pendingDrag.edge,
        pendingDrag.timelineElement,
        pendingDrag.clientX,
      )
    })
  }

  const startRangeDrag = (
    event: PointerEvent<HTMLElement>,
    item: TalkRuleTimelineItem,
    edge: 'end' | 'start',
  ) => {
    event.preventDefault()
    lastDragTimeRef.current = null
    event.currentTarget.setPointerCapture(event.pointerId)
    scheduleRangeUpdate(event, item, edge)
  }

  const reorderTrack = (targetItem: TalkRuleTimelineItem) => {
    const draggingTrack = draggingTrackRef.current
    if (!draggingTrack || draggingTrack.groupKey !== targetItem.groupKey) {
      return
    }
    if (draggingTrack.index === targetItem.index || !targetItem.range) {
      return
    }

    const nextItems = moveTalkRuleItem(items, draggingTrack.index, targetItem.index)
    onItemsChange(nextItems, { changedIndex: targetItem.index })
    draggingTrackRef.current = null
  }

  return (
    <div className="overflow-hidden rounded-lg border bg-muted/20">
      <div className="border-b bg-background/70 px-3 py-2">
        <div className="flex flex-wrap items-center gap-2">
          <h5 className="text-sm font-semibold">时间轴视图</h5>
          <Badge variant="secondary">聊天区域 {timelineGroups.length}</Badge>
          <Badge variant="outline">轨道 {timelineItems.length}</Badge>
        </div>
      </div>
      <div className="overflow-x-auto">
        <div className="min-w-[540px] p-2.5">
          <div className="grid grid-cols-[5.5rem_minmax(16rem,1fr)_6.5rem] gap-2 pb-1.5 text-[11px] text-muted-foreground">
            <div>轨道</div>
            <div className="relative h-4 px-1">
              {TIMELINE_TICKS.map((hour) => (
                <span
                  key={hour}
                  className="absolute -translate-x-1/2"
                  style={{ left: `${(hour / 24) * 100}%` }}
                >
                  {hour.toString().padStart(2, '0')}:00
                </span>
              ))}
            </div>
            <div>频率</div>
          </div>
          <div className="space-y-2.5">
            {timelineGroups.map((group) => (
              <div key={group.key} className="overflow-hidden rounded-lg border bg-card/60">
                <div className="flex flex-wrap items-center gap-1.5 border-b bg-background/70 px-2 py-1.5">
                  <div className="min-w-0 flex-1 truncate text-xs font-semibold">{group.label}</div>
                  <Badge variant="secondary">{group.scopeLabel}</Badge>
                  <Badge variant="outline">{group.items.length} 轨道</Badge>
                  <div className="ml-auto flex flex-wrap gap-1">
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-7 px-2 text-xs"
                      onClick={() => onAddItem(createTalkRuleForGroup(group, '00:00-23:59'))}
                    >
                      <Plus className="mr-1 h-3 w-3" />
                      时间段
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-7 px-2 text-xs"
                      disabled={group.hasFallback}
                      onClick={() => onAddItem(createTalkRuleForGroup(group, ''))}
                    >
                      <Plus className="mr-1 h-3 w-3" />
                      兜底
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-7 px-2 text-xs"
                      disabled={group.hasWildcard}
                      onClick={() => onAddItem(createTalkRuleForGroup(group, '*'))}
                    >
                      <Plus className="mr-1 h-3 w-3" />
                      *
                    </Button>
                  </div>
                </div>
                <div className="space-y-1.5 p-2">
                  {group.items.map((item, trackIndex) => (
                    <div
                      key={item.index}
                      className="grid min-h-12 grid-cols-[5.5rem_minmax(16rem,1fr)_6.5rem] items-center gap-2 rounded-md bg-muted/25 px-2 py-1.5"
                      onDragOver={(event) => {
                        if (item.range && draggingTrackRef.current?.groupKey === item.groupKey) {
                          event.preventDefault()
                        }
                      }}
                      onDrop={(event) => {
                        event.preventDefault()
                        reorderTrack(item)
                      }}
                    >
                      <div className="flex min-w-0 items-center gap-2">
                        <button
                          type="button"
                          draggable={Boolean(item.range)}
                          disabled={!item.range}
                          className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:cursor-not-allowed disabled:opacity-35"
                          aria-label={`拖动${group.label}轨道 ${trackIndex + 1} 调整顺序`}
                          onDragEnd={() => {
                            draggingTrackRef.current = null
                          }}
                          onDragStart={(event) => {
                            if (!item.range) {
                              event.preventDefault()
                              return
                            }
                            draggingTrackRef.current = {
                              groupKey: item.groupKey,
                              index: item.index,
                            }
                            event.dataTransfer.effectAllowed = 'move'
                          }}
                        >
                          <GripVertical className="h-3.5 w-3.5" />
                        </button>
                        <div className="min-w-0">
                          <div className="truncate text-xs font-medium">轨道 {trackIndex + 1}</div>
                        <div
                          className={item.invalidTime ? 'text-[11px] text-destructive' : 'text-[11px] text-muted-foreground'}
                        >
                          {item.timeLabel}
                        </div>
                        </div>
                      </div>
                      <div
                        className="relative h-7 rounded-md border bg-background"
                        data-talk-timeline-track
                      >
                        {TIMELINE_TICKS.slice(1, -1).map((hour) => (
                          <span
                            key={hour}
                            className="absolute top-0 h-full border-l border-dashed border-muted-foreground/20"
                            style={{ left: `${(hour / 24) * 100}%` }}
                          />
                        ))}
                        {item.segments.map((segment, segmentIndex) => (
                          <span
                            key={segmentIndex}
                            className={`absolute top-1/2 h-4 -translate-y-1/2 rounded-sm ${talkValueColor(item.value)} ${
                              item.invalidTime ? 'opacity-35' : 'opacity-85'
                            }`}
                            style={{
                              left: `${segment.left}%`,
                              width: `${segment.width}%`,
                            }}
                            title={`${item.timeLabel} · 频率 ${item.value.toFixed(2)}`}
                          />
                        ))}
                        {item.range && !item.invalidTime && (
                          <>
                            <button
                              type="button"
                              className="absolute top-1/2 h-6 w-2 -translate-x-1/2 -translate-y-1/2 rounded-sm border border-background bg-foreground/80 shadow-sm cursor-ew-resize"
                              style={{ left: `${(item.range.start / DAY_MINUTES) * 100}%` }}
                              aria-label={`调整${group.label}轨道 ${trackIndex + 1} 开始时间`}
                              onPointerDown={(event) => startRangeDrag(event, item, 'start')}
                              onPointerMove={(event) => {
                                if (event.currentTarget.hasPointerCapture(event.pointerId)) {
                                  scheduleRangeUpdate(event, item, 'start')
                                }
                              }}
                              onPointerUp={(event) => {
                                pendingDragRef.current = null
                                event.currentTarget.releasePointerCapture(event.pointerId)
                              }}
                            />
                            <button
                              type="button"
                              className="absolute top-1/2 h-6 w-2 -translate-x-1/2 -translate-y-1/2 rounded-sm border border-background bg-foreground/80 shadow-sm cursor-ew-resize"
                              style={{ left: `${((item.range.end + 1) / DAY_MINUTES) * 100}%` }}
                              aria-label={`调整${group.label}轨道 ${trackIndex + 1} 结束时间`}
                              onPointerDown={(event) => startRangeDrag(event, item, 'end')}
                              onPointerMove={(event) => {
                                if (event.currentTarget.hasPointerCapture(event.pointerId)) {
                                  scheduleRangeUpdate(event, item, 'end')
                                }
                              }}
                              onPointerUp={(event) => {
                                pendingDragRef.current = null
                                event.currentTarget.releasePointerCapture(event.pointerId)
                              }}
                            />
                          </>
                        )}
                      </div>
                      <div className="grid grid-cols-[minmax(0,1fr)_1.75rem] items-center gap-1.5">
                        <div>
                          <Slider
                            value={[item.value]}
                            min={0}
                            max={1}
                            step={0.01}
                            onValueChange={(values) => onItemFieldChange(item.index, 'value', values[0])}
                            data-dashboard-slider="config"
                            data-dashboard-slider-value-format="fixed-2"
                          />
                        </div>
                        <Button
                          type="button"
                          size="icon"
                          variant="ghost"
                          className="h-7 w-7 text-destructive hover:text-destructive"
                          aria-label={`删除${group.label} 轨道 ${trackIndex + 1}`}
                          onClick={() => onRemoveItem(item.index)}
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

function TalkValueGroupedRuleEditor({
  emptyText,
  items,
  onItemFieldChange,
  onItemsChange,
  onRemoveItem,
}: {
  emptyText: string
  items: Record<string, unknown>[]
  onItemFieldChange: (index: number, fieldName: string, fieldValue: unknown) => void
  onItemsChange: (
    items: Record<string, unknown>[],
    context?: { addedIndex?: number; changedIndex?: number },
  ) => void
  onRemoveItem: (index: number) => void
}) {
  const timelineGroups = groupTalkTimelineItems(buildTalkTimelineItems(items))
  if (timelineGroups.length === 0) {
    return (
      <div className="rounded-md border border-dashed border-muted-foreground/25 bg-muted/10 p-6 text-center text-sm text-muted-foreground">
        {emptyText}
      </div>
    )
  }

  const updateGroupField = (
    group: TalkRuleTimelineGroup,
    fieldName: 'item_id' | 'platform' | 'rule_type',
    fieldValue: string,
  ) => {
    const groupIndexes = new Set(group.items.map((item) => item.index))
    const nextItems = items.map((item, index) => {
      if (!groupIndexes.has(index)) {
        return item
      }
      return {
        ...item,
        [fieldName]: fieldValue,
      }
    })
    onItemsChange(nextItems, { changedIndex: group.items[0]?.index })
  }

  return (
    <div className="space-y-4">
      {timelineGroups.map((group) => (
        <div key={group.key} className="space-y-4 rounded-lg border bg-card/40 p-4">
          <div className="flex flex-wrap items-center gap-2">
            <div className="min-w-0 flex-1">
              <div className="truncate text-sm font-semibold">{group.label}</div>
              <div className="text-xs text-muted-foreground">{group.items.length} 条轨道</div>
            </div>
            <Badge variant="secondary">{group.scopeLabel}</Badge>
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            <div className="space-y-2">
              <Label className="text-xs font-medium">平台</Label>
              <Input
                value={group.platform}
                placeholder="留空表示全局，* 表示通配"
                onChange={(event) => updateGroupField(group, 'platform', event.target.value)}
              />
            </div>
            <div className="space-y-2">
              <Label className="text-xs font-medium">聊天流 ID</Label>
              <Input
                value={group.itemId}
                placeholder="留空表示全局，* 表示通配"
                onChange={(event) => updateGroupField(group, 'item_id', event.target.value)}
              />
            </div>
            <div className="space-y-2">
              <Label className="text-xs font-medium">聊天类型</Label>
              <Select
                value={group.ruleType === 'private' ? 'private' : 'group'}
                onValueChange={(value) => updateGroupField(group, 'rule_type', value)}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="group">群聊</SelectItem>
                  <SelectItem value="private">私聊</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-2">
            {group.items.map((item, trackIndex) => {
              const isFallback = item.rawTime === ''
              const isWildcard = item.rawTime === '*'
              const canUseFallback = isFallback || !group.hasFallback
              const canUseWildcard = isWildcard || !group.hasWildcard

              return (
                <div
                  key={item.index}
                  className="grid gap-3 rounded-md border bg-muted/20 p-3 lg:grid-cols-[7rem_minmax(16rem,1fr)_14rem_2.5rem]"
                >
                  <div className="flex items-center">
                    <span className="text-sm font-medium">轨道 {trackIndex + 1}</span>
                  </div>
                  <div className="space-y-2">
                    <div className="grid grid-cols-3 gap-2">
                      <Button
                        type="button"
                        size="sm"
                        variant={isFallback ? 'default' : 'outline'}
                        disabled={!canUseFallback}
                        onClick={() => onItemFieldChange(item.index, 'time', '')}
                      >
                        兜底
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant={!isFallback && !isWildcard ? 'default' : 'outline'}
                        onClick={() =>
                          onItemFieldChange(
                            item.index,
                            'time',
                            !isFallback && !isWildcard ? item.rawTime : '00:00-23:59',
                          )
                        }
                      >
                        时间段
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant={isWildcard ? 'default' : 'outline'}
                        disabled={!canUseWildcard}
                        onClick={() => onItemFieldChange(item.index, 'time', '*')}
                      >
                        *
                      </Button>
                    </div>
                    <Input
                      value={!isFallback && !isWildcard ? item.rawTime : ''}
                      disabled={isFallback || isWildcard}
                      placeholder="HH:MM-HH:MM"
                      onChange={(event) => onItemFieldChange(item.index, 'time', event.target.value)}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label className="text-xs font-medium">发言频率</Label>
                    <div className="flex items-center gap-2">
                      <Slider
                        value={[item.value]}
                        min={0}
                        max={1}
                        step={0.01}
                        onValueChange={(values) => onItemFieldChange(item.index, 'value', values[0])}
                        data-dashboard-slider="config"
                        data-dashboard-slider-value-format="fixed-2"
                      />
                      <Input
                        type="number"
                        min={0}
                        max={1}
                        step={0.01}
                        value={item.value}
                        onChange={(event) => {
                          const nextValue = Number(event.target.value)
                          if (Number.isFinite(nextValue)) {
                            onItemFieldChange(item.index, 'value', Math.max(0, Math.min(1, nextValue)))
                          }
                        }}
                        className="h-8 w-20 text-right"
                      />
                    </div>
                  </div>
                  <div className="flex items-center justify-end">
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      className="text-destructive hover:text-destructive"
                      aria-label={`删除${group.label}轨道 ${trackIndex + 1}`}
                      onClick={() => onRemoveItem(item.index)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}

type TalkValueEditorMode = 'grouped' | 'timeline'

const createEmptyTalkRuleDraft = () => ({
  itemId: '',
  platform: '',
  ruleType: 'group',
})

function TalkValueRuleEditor({
  emptyText,
  items,
  onAddItem,
  onItemFieldChange,
  onItemsChange,
  onRemoveItem,
}: {
  emptyText: string
  items: Record<string, unknown>[]
  onAddItem: (item?: Record<string, unknown>) => void
  onItemFieldChange: (index: number, fieldName: string, fieldValue: unknown) => void
  onItemsChange: (
    items: Record<string, unknown>[],
    context?: { addedIndex?: number; changedIndex?: number },
  ) => void
  onRemoveItem: (index: number) => void
}) {
  const [mode, setMode] = useState<TalkValueEditorMode>('timeline')
  const [addDialogOpen, setAddDialogOpen] = useState(false)
  const [addDraft, setAddDraft] = useState(createEmptyTalkRuleDraft)
  const canSubmitAddRule = addDraft.platform.trim().length > 0 && addDraft.itemId.trim().length > 0

  const handleAddDialogOpenChange = (open: boolean) => {
    setAddDialogOpen(open)
    if (!open) {
      setAddDraft(createEmptyTalkRuleDraft())
    }
  }

  const handleSubmitAddRule = () => {
    if (!canSubmitAddRule) {
      return
    }

    onAddItem({
      platform: addDraft.platform.trim(),
      item_id: addDraft.itemId.trim(),
      rule_type: addDraft.ruleType,
      time: '00:00-23:59',
      value: 0.5,
    })
    handleAddDialogOpenChange(false)
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <Button
          type="button"
          size="sm"
          variant="outline"
          onClick={() => setAddDialogOpen(true)}
        >
          <Plus className="mr-1 h-4 w-4" />
          添加发言频率规则
        </Button>
        <div className="inline-flex rounded-md border bg-background p-1">
          <Button
            type="button"
            size="sm"
            variant={mode === 'timeline' ? 'default' : 'ghost'}
            onClick={() => setMode('timeline')}
          >
            可视化轨道
          </Button>
          <Button
            type="button"
            size="sm"
            variant={mode === 'grouped' ? 'default' : 'ghost'}
            onClick={() => setMode('grouped')}
          >
            合并编辑
          </Button>
        </div>
      </div>
      <Dialog open={addDialogOpen} onOpenChange={handleAddDialogOpenChange}>
        <DialogContent className="sm:max-w-md" confirmOnEnter>
          <DialogHeader>
            <DialogTitle>添加发言频率规则</DialogTitle>
            <DialogDescription>
              先指定平台、聊天流 ID 和聊天类型，再创建该聊天区域的默认时间段轨道。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label className="text-xs font-medium">平台</Label>
              <Input
                value={addDraft.platform}
                placeholder="例如 qq"
                onChange={(event) =>
                  setAddDraft((current) => ({
                    ...current,
                    platform: event.target.value,
                  }))
                }
              />
            </div>
            <div className="space-y-2">
              <Label className="text-xs font-medium">聊天流 ID</Label>
              <Input
                value={addDraft.itemId}
                placeholder="群号或用户 ID"
                onChange={(event) =>
                  setAddDraft((current) => ({
                    ...current,
                    itemId: event.target.value,
                  }))
                }
              />
            </div>
            <div className="space-y-2">
              <Label className="text-xs font-medium">聊天类型</Label>
              <Select
                value={addDraft.ruleType}
                onValueChange={(value) =>
                  setAddDraft((current) => ({
                    ...current,
                    ruleType: value,
                  }))
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="group">群聊</SelectItem>
                  <SelectItem value="private">私聊</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => handleAddDialogOpenChange(false)}
            >
              取消
            </Button>
            <Button
              type="button"
              data-dialog-action="confirm"
              disabled={!canSubmitAddRule}
              onClick={handleSubmitAddRule}
            >
              添加
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      {mode === 'timeline' ? (
        <TalkValueTimelineOverview
          items={items}
          onAddItem={onAddItem}
          onItemFieldChange={onItemFieldChange}
          onItemsChange={onItemsChange}
          onRemoveItem={onRemoveItem}
        />
      ) : (
        <TalkValueGroupedRuleEditor
          emptyText={emptyText}
          items={items}
          onItemFieldChange={onItemFieldChange}
          onItemsChange={onItemsChange}
          onRemoveItem={onRemoveItem}
        />
      )}
    </div>
  )
}

const truncate = (text: string, max = 32) => {
  if (text.length <= max) return text
  return `${text.slice(0, max)}…`
}

const collectStringList = (value: unknown): string[] => {
  if (!Array.isArray(value)) return []
  return value
    .map((item) => (typeof item === 'string' ? item.trim() : ''))
    .filter((item) => item.length > 0)
}

const normalizeExpressionRuleType = (value: unknown): ExpressionRuleType => {
  return value === 'private' ? 'private' : 'group'
}

const normalizeExpressionTarget = (value: unknown): ExpressionGroupTarget => {
  const source =
    value && typeof value === 'object'
      ? (value as Record<string, unknown>)
      : {}
  return {
    platform:
      typeof source.platform === 'string' ? source.platform.trim() : 'qq',
    item_id:
      typeof source.item_id === 'string' ? source.item_id.trim() : '',
    rule_type: normalizeExpressionRuleType(source.rule_type ?? source.type),
  }
}

const normalizeExpressionGroups = (value: unknown): ExpressionGroupValue[] => {
  if (!Array.isArray(value)) return []
  return value.map((item) => {
    const source =
      item && typeof item === 'object'
        ? (item as Record<string, unknown>)
        : {}
    let rawMembers: unknown[] = []
    if (Array.isArray(source.targets)) {
      rawMembers = source.targets
    } else if (Array.isArray(source.expression_groups)) {
      rawMembers = source.expression_groups
    } else if (Array.isArray(source.jargon_groups)) {
      rawMembers = source.jargon_groups
    } else if (Array.isArray(source.behavior_groups)) {
      rawMembers = source.behavior_groups
    }
    const members = rawMembers.map(normalizeExpressionTarget)
    return { targets: members }
  })
}

const createExpressionTarget = (): ExpressionGroupTarget => ({
  platform: 'qq',
  item_id: '',
  rule_type: 'group',
})

const formatExpressionTarget = (target: ExpressionGroupTarget): string => {
  const platform = target.platform.trim()
  const itemId = target.item_id.trim()
  const rule = ruleTypeLabel(target.rule_type)
  if (!platform && !itemId) return `全局 · ${rule}`
  if (!itemId) return `${platform} · ${rule}`
  return `${platform}:${itemId} · ${rule}`
}

const normalizePlatformAccounts = (value: unknown): string[] => {
  if (!Array.isArray(value)) return []
  return value.map((item) => String(item ?? ''))
}

const parsePlatformAccount = (value: string): PlatformAccountRow => {
  const separatorIndex = value.indexOf(':')
  if (separatorIndex < 0) {
    return { platform: '', account: value }
  }
  return {
    platform: value.slice(0, separatorIndex),
    account: value.slice(separatorIndex + 1),
  }
}

const formatPlatformAccount = (row: PlatformAccountRow): string => {
  const platform = row.platform.trim()
  const account = row.account.trim()
  if (!platform) return account
  if (!account) return `${platform}:`
  return `${platform}:${account}`
}

interface StringListHookOptions {
  addLabel: string
  emptyText: string
  label: string
  multiline?: boolean
  placeholder?: string
}

function createStringListHook(options: StringListHookOptions): FieldHookComponent {
  return ({ onChange, schema, value }) => {
    const items = Array.isArray(value) ? value.map((item) => String(item ?? '')) : []

    const updateItems = (nextItems: string[]) => {
      onChange?.(nextItems)
    }

    const addItem = () => {
      updateItems([...items, ''])
    }

    const removeItem = (itemIndex: number) => {
      updateItems(items.filter((_, index) => index !== itemIndex))
    }

    const updateItem = (itemIndex: number, nextValue: string) => {
      updateItems(items.map((item, index) => (index === itemIndex ? nextValue : item)))
    }

    const InputComponent = options.multiline ? Textarea : Input

    return (
      <div className="space-y-3">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <Label className={fieldTitleClassName(schema, 'text-[15px] leading-6')}>
            {options.label}
          </Label>
          <Button type="button" size="sm" variant="outline" onClick={addItem}>
            <Plus className="mr-2 h-4 w-4" />
            {options.addLabel}
          </Button>
        </div>

        {items.length === 0 ? (
          <div className="rounded-md border border-dashed bg-muted/30 px-4 py-5 text-center text-sm text-muted-foreground">
            {options.emptyText}
          </div>
        ) : (
          <div className="space-y-2">
            {items.map((item, itemIndex) => (
              <div
                key={itemIndex}
                className="grid gap-2 rounded-md border bg-muted/20 p-3 sm:grid-cols-[minmax(0,1fr)_2.5rem]"
              >
                <InputComponent
                  value={item}
                  placeholder={options.placeholder}
                  onChange={(event) => updateItem(itemIndex, event.target.value)}
                  {...(options.multiline ? { rows: 2 } : {})}
                />
                <div className="flex items-start justify-end">
                  <Button
                    type="button"
                    size="icon"
                    variant="ghost"
                    aria-label={`删除${options.label} ${itemIndex + 1}`}
                    onClick={() => removeItem(itemIndex)}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    )
  }
}

export const AliasNamesHook = createStringListHook({
  addLabel: '添加别名',
  emptyText: '暂无别名。',
  label: '别名',
  placeholder: '小麦',
})

export const MultipleReplyStyleHook = createStringListHook({
  addLabel: '添加表达风格',
  emptyText: '暂无备用表达风格。',
  label: '备用表达风格',
  multiline: true,
  placeholder: '输入一种备用表达风格',
})

export const ChatTalkValueRulesHook = createListItemEditorHook({
  addLabel: '添加发言频率规则',
  addButtonPlacement: 'none',
  collapseWhen: ({ parentValues }) => parentValues?.enable_talk_value_rules === false,
  collapsedText: '动态发言频率规则未启用，规则列表已折叠。展开后仍可查看或编辑已有规则。',
  expandLabel: '展开规则',
  collapseLabel: '折叠规则',
  helperText: '可按平台/聊天流/时段分别配置发言频率。平台和聊天流都空表示全局；只填平台或聊天流表示对应默认值；* 表示通配覆盖。时间留空表示兜底，* 表示强制全天。',
  emptyText: '尚未配置任何规则，将使用全局默认频率。',
  collapseButtonDisplay: 'icon',
  fieldRows: [
    ['platform', 'item_id', 'rule_type'],
    ['time', 'value'],
  ],
  normalizeItems: normalizeTalkRuleItems,
  renderItems: ({
    emptyText,
    items,
    onAddItem,
    onItemFieldChange,
    onItemsChange,
    onRemoveItem,
  }) => (
    <TalkValueRuleEditor
      emptyText={emptyText}
      items={items}
      onAddItem={onAddItem}
      onItemFieldChange={onItemFieldChange}
      onItemsChange={onItemsChange}
      onRemoveItem={onRemoveItem}
    />
  ),
  itemTitle: (item) => {
    const rawTime = typeof item.time === 'string' ? item.time.trim() : ''
    const time = rawTime === '' ? '兜底' : rawTime === '*' ? '强制全天' : rawTime
    const value =
      typeof item.value === 'number' ? item.value.toFixed(2) : '—'
    return `${platformLabel(item)} · ${ruleTypeLabel(item.rule_type)} · ${time} · 频率 ${value}`
  },
})

export const ChatPromptsHook = createListItemEditorHook({
  addLabel: '添加额外 Prompt',
  helperText: '为指定平台和聊天流添加额外提示。platform、item_id 和 prompt 同时留空时表示空条目；填写任意一项后这三项都需要填写。',
  emptyText: '尚未配置任何聊天额外 Prompt。',
  addButtonPlacement: 'top',
  fieldRows: [
    ['platform', 'item_id'],
    ['rule_type'],
  ],
  fieldSchemaOverrides: {
    item_id: {
      'x-input-width': '6.5rem',
      'x-layout': 'inline-right',
    },
    platform: {
      'x-input-width': '5.5rem',
      'x-layout': 'inline-right',
    },
    prompt: {
      'x-textarea-min-height': 38,
      'x-textarea-rows': 1,
    },
    rule_type: {
      'x-input-width': '5.5rem',
      'x-layout': 'inline-right',
    },
  },
  iconName: 'file-text',
  itemTitle: (item) => {
    return `${platformLabel(item)} · ${ruleTypeLabel(item.rule_type)}`
  },
})

export const ExpressionLearningListHook = createListItemEditorHook({
  addLabel: '添加学习规则',
  infoText: '可以单独为每个聊天开启学习和使用，留空作为兜底，*作为全部覆盖',
  emptyText: '尚未配置任何学习规则。',
  fallbackNestedSchema: LEARNING_ITEM_FALLBACK_SCHEMA,
  fieldRows: [
    ['platform', 'item_id', 'type'],
    ['use', 'learn'],
  ],
  itemTitle: (item) => {
    const flags: string[] = []
    if (item.use) flags.push('使用')
    if (item.learn) flags.push('学习')
    const flagText = flags.length ? flags.join(' / ') : '使用和学习均关闭'
    return `${platformLabel(item)} · ${ruleTypeLabel(item.type)} · ${flagText}`
  },
})

export const JargonLearningListHook = ExpressionLearningListHook

export const BehaviorLearningListHook = createListItemEditorHook({
  addLabel: '添加行为学习规则',
  infoText: '可以单独为每个聊天开启行为经验的学习和使用，留空作为兜底，* 作为全部覆盖。',
  emptyText: '尚未配置任何行为学习规则。',
  fallbackNestedSchema: LEARNING_ITEM_FALLBACK_SCHEMA,
  fieldRows: [
    ['platform', 'item_id', 'type'],
    ['use', 'learn'],
  ],
  itemTitle: (item) => {
    const flags: string[] = []
    if (item.use) flags.push('使用')
    if (item.learn) flags.push('学习')
    const flagText = flags.length ? flags.join(' / ') : '使用和学习均关闭'
    return `${platformLabel(item)} · ${ruleTypeLabel(item.type)} · ${flagText}`
  },
})

export const FocusWhitelistHook = createListItemEditorHook({
  addLabel: '添加 Focus 白名单',
  infoText: '配置后只有命中的聊天流会进入 Focus；留空表示所有符合聊天类型开关的聊天都可进入 Focus。',
  emptyText: '尚未配置 Focus 白名单。',
  fallbackNestedSchema: LEARNING_ITEM_FALLBACK_SCHEMA,
  fieldRows: [['platform', 'item_id', 'type']],
  itemTitle: (item) => {
    return `${platformLabel(item)} · ${ruleTypeLabel(item.type)}`
  },
})

export const BotPlatformsHook: FieldHookComponent = ({ onChange, value }) => {
  const platforms = normalizePlatformAccounts(value)
  const rows = platforms.map(parsePlatformAccount)

  const updateRows = (nextRows: PlatformAccountRow[]) => {
    onChange?.(nextRows.map(formatPlatformAccount))
  }

  const addRow = () => {
    updateRows([...rows, { platform: '', account: '' }])
  }

  const removeRow = (rowIndex: number) => {
    updateRows(rows.filter((_, index) => index !== rowIndex))
  }

  const updateRow = (rowIndex: number, patch: Partial<PlatformAccountRow>) => {
    updateRows(
      rows.map((row, index) =>
        index === rowIndex ? { ...row, ...patch } : row
      )
    )
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="space-y-1">
          <Label className="text-sm font-medium">其他平台</Label>
          <p className="text-xs text-muted-foreground">
            每行保存为 platform:account，例如 wx:114514。
          </p>
        </div>
        <Button type="button" size="icon" variant="outline" aria-label="添加平台" title="添加平台" onClick={addRow}>
          <Plus className="h-4 w-4" />
        </Button>
      </div>

      {rows.length === 0 ? (
        <div className="rounded-md border border-dashed bg-muted/30 px-4 py-5 text-center text-sm text-muted-foreground">
          暂无其他平台账号。
        </div>
      ) : (
        <div className="space-y-2">
          {rows.map((row, rowIndex) => (
            <div
              key={rowIndex}
              className={PLATFORM_ACCOUNT_ROW_GRID_CLASS}
            >
              <div className="min-w-0 space-y-1">
                <Label className="text-xs">平台</Label>
                <Input
                  className="min-w-0"
                  value={row.platform}
                  placeholder="wx"
                  onChange={(event) =>
                    updateRow(rowIndex, { platform: event.target.value })
                  }
                />
              </div>
              <div className="min-w-0 space-y-1">
                <Label className="text-xs">账号</Label>
                <Input
                  className="min-w-0 font-mono"
                  value={row.account}
                  placeholder="114514"
                  onChange={(event) =>
                    updateRow(rowIndex, { account: event.target.value })
                  }
                />
              </div>
              <div className="flex shrink-0 items-end justify-end">
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  aria-label={`删除其他平台 ${rowIndex + 1}`}
                  onClick={() => removeRow(rowIndex)}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export const HiddenFieldHook: FieldHookComponent = () => null

export const BotPlatformAccountsHook: FieldHookComponent = ({
  onChange,
  onParentChange,
  parentValues,
  value,
}) => {
  const primaryPlatform = typeof value === 'string' ? value : ''
  const qqAccountValue = parentValues?.qq_account
  const qqAccount =
    typeof qqAccountValue === 'string' || typeof qqAccountValue === 'number'
      ? String(qqAccountValue)
      : ''
  const platforms = normalizePlatformAccounts(parentValues?.platforms)
  const rows = platforms.map(parsePlatformAccount)

  const updateRows = (nextRows: PlatformAccountRow[]) => {
    onParentChange?.('platforms', nextRows.map(formatPlatformAccount))
  }

  const addRow = () => {
    updateRows([...rows, { platform: '', account: '' }])
  }

  const removeRow = (rowIndex: number) => {
    updateRows(rows.filter((_, index) => index !== rowIndex))
  }

  const updateRow = (rowIndex: number, patch: Partial<PlatformAccountRow>) => {
    updateRows(rows.map((row, index) => (index === rowIndex ? { ...row, ...patch } : row)))
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0 space-y-1">
          <Label className="text-[15px] font-semibold leading-6">平台账号</Label>
        </div>
        <Button
          type="button"
          size="icon"
          variant="outline"
          className="shrink-0"
          aria-label="添加平台"
          title="添加平台"
          onClick={addRow}
        >
          <Plus className="h-4 w-4" />
        </Button>
      </div>

      <div className="space-y-2">
        <div className={PLATFORM_ACCOUNT_ROW_GRID_CLASS}>
          <div className="min-w-0 space-y-1">
            <Label className="text-xs">平台</Label>
            <Input
              className="min-w-0"
              value={primaryPlatform}
              placeholder="qq"
              onChange={(event) => onChange?.(event.target.value)}
            />
          </div>
          <div className="min-w-0 space-y-1">
            <Label className="text-xs">账号</Label>
            <Input
              className="min-w-0 font-mono"
              value={qqAccount}
              placeholder="2814567326"
              onChange={(event) => onParentChange?.('qq_account', event.target.value)}
            />
          </div>
          <div className="flex shrink-0 items-end justify-end">
            <span className="rounded-md bg-primary/10 px-2 py-1 text-xs font-medium text-primary">
              主
            </span>
          </div>
        </div>

        {rows.map((row, rowIndex) => (
          <div
            key={rowIndex}
            className={PLATFORM_ACCOUNT_ROW_GRID_CLASS}
          >
            <div className="min-w-0 space-y-1">
              <Label className="text-xs">平台</Label>
              <Input
                className="min-w-0"
                value={row.platform}
                placeholder="wx"
                onChange={(event) => updateRow(rowIndex, { platform: event.target.value })}
              />
            </div>
            <div className="min-w-0 space-y-1">
              <Label className="text-xs">账号</Label>
              <Input
                className="min-w-0 font-mono"
                value={row.account}
                placeholder="114514"
                onChange={(event) => updateRow(rowIndex, { account: event.target.value })}
              />
            </div>
            <div className="flex shrink-0 items-end justify-end">
              <Button
                type="button"
                size="icon"
                variant="ghost"
                aria-label={`删除其他平台 ${rowIndex + 1}`}
                onClick={() => removeRow(rowIndex)}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

export const KeywordRulesHook = createListItemEditorHook({
  addLabel: '添加关键词规则',
  helperText: '匹配命中后会用 reaction 内容作为额外上下文。keywords 至少填一条，或使用正则模式。',
  emptyText: '尚未添加任何关键词规则。',
  itemTitle: (item) => {
    const keywords = collectStringList(item.keywords)
    const regex = collectStringList(item.regex)
    const reaction =
      typeof item.reaction === 'string' ? item.reaction.trim() : ''
    const left = keywords.length
      ? `关键词 ${keywords.length} 条`
      : regex.length
        ? `正则 ${regex.length} 条`
        : '未配置匹配项'
    const right = reaction ? `→ ${truncate(reaction)}` : '→ 未填写反应'
    return `${left} ${right}`
  },
})

export const RegexRulesHook = createListItemEditorHook({
  addLabel: '添加正则规则',
  helperText: '正则模式按 Python 语法编写，命中时把 reaction 作为提示注入。',
  emptyText: '尚未添加任何正则规则。',
  itemTitle: (item) => {
    const regex = collectStringList(item.regex)
    const keywords = collectStringList(item.keywords)
    const reaction =
      typeof item.reaction === 'string' ? item.reaction.trim() : ''
    const left = regex.length
      ? `正则 ${regex.length} 条`
      : keywords.length
        ? `关键词 ${keywords.length} 条`
        : '未配置匹配项'
    const right = reaction ? `→ ${truncate(reaction)}` : '→ 未填写反应'
    return `${left} ${right}`
  },
})

export const ExpressionGroupsHook: FieldHookComponent = ({ fieldPath, onChange, schema, value }) => {
  const groups = normalizeExpressionGroups(value)
  const isJargonGroup = fieldPath?.includes('jargon') ?? false
  const isBehaviorGroup = fieldPath?.includes('behavior') ?? false
  const isSharedMemoryGroup = fieldPath?.includes('shared_memory_groups') ?? false
  const isFocusGroup = fieldPath?.includes('focus_groups') ?? false
  const displaysAsSection =
    isSharedMemoryGroup &&
    Boolean(schema && 'x-display-as-section' in schema && schema['x-display-as-section'])
  const groupLabel = isSharedMemoryGroup
    ? '共享记忆组'
    : isFocusGroup
      ? 'Focus 互通组'
      : isBehaviorGroup
        ? '行为互通组'
        : isJargonGroup
          ? '黑话互通组'
          : '表达互通组'
  const learnedContentLabel = isBehaviorGroup ? '行为经验' : isJargonGroup ? '黑话' : '表达方式'
  const helperText = isSharedMemoryGroup
    ? '把几个群聊或私聊放进同一组后，麦麦在其中任意一个聊天里回忆长期记忆时，会一起参考同组聊天的记忆；新产生的内容仍记在原来的聊天里。'
    : isFocusGroup
      ? '配置后只有同组聊天流共享 Focus，不同组可以分别进入 Focus。'
    : `每个互通组内的聊天流会共享已学习的${learnedContentLabel}。`
  const [collapsedGroups, setCollapsedGroups] = useState<Set<number>>(() => new Set())

  const updateGroups = (nextGroups: ExpressionGroupValue[]) => {
    onChange?.(
      nextGroups.map((group) => ({
        targets: group.targets.map((target) => ({
          platform: target.platform,
          item_id: target.item_id,
          rule_type: target.rule_type,
        })),
      }))
    )
  }

  const addGroup = () => {
    updateGroups([...groups, { targets: [createExpressionTarget()] }])
  }

  const toggleGroupCollapsed = (groupIndex: number) => {
    setCollapsedGroups((current) => {
      const next = new Set(current)
      if (next.has(groupIndex)) {
        next.delete(groupIndex)
      } else {
        next.add(groupIndex)
      }
      return next
    })
  }

  const removeGroup = (groupIndex: number) => {
    updateGroups(groups.filter((_, index) => index !== groupIndex))
  }

  const addMember = (groupIndex: number) => {
    updateGroups(
      groups.map((group, index) =>
        index === groupIndex
          ? {
              targets: [
                ...group.targets,
                createExpressionTarget(),
              ],
            }
          : group
      )
    )
  }

  const removeMember = (groupIndex: number, memberIndex: number) => {
    updateGroups(
      groups.map((group, index) =>
        index === groupIndex
          ? {
              targets: group.targets.filter(
                (_, currentMemberIndex) => currentMemberIndex !== memberIndex
              ),
            }
          : group
      )
    )
  }

  const updateMember = (
    groupIndex: number,
    memberIndex: number,
    patch: Partial<ExpressionGroupTarget>
  ) => {
    updateGroups(
      groups.map((group, index) =>
        index === groupIndex
          ? {
              targets: group.targets.map(
                (member, currentMemberIndex) =>
                  currentMemberIndex === memberIndex
                    ? { ...member, ...patch }
                    : member
              ),
            }
          : group
      )
    )
  }

  return (
    <div className={displaysAsSection ? 'space-y-3' : 'space-y-3 rounded-lg border bg-card p-4 sm:p-5'}>
      <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1">
          {!displaysAsSection && <h3 className="text-base font-semibold">{groupLabel}</h3>}
          <p className="text-sm text-muted-foreground">
            {helperText}
          </p>
        </div>
        <Button
          type="button"
          size="icon"
          variant="outline"
          aria-label={`添加${groupLabel}`}
          title={`添加${groupLabel}`}
          onClick={addGroup}
        >
          <Plus className="h-4 w-4" />
        </Button>
      </div>

      {groups.length === 0 ? (
        <div className="rounded-md border border-dashed bg-muted/30 px-4 py-8 text-center text-sm text-muted-foreground">
          暂无{groupLabel}，点击上方按钮开始配置。
        </div>
      ) : (
        <div className="space-y-2">
          {groups.map((group, groupIndex) => {
            const isCollapsed = collapsedGroups.has(groupIndex)

            return (
              <div
                key={groupIndex}
                className="space-y-2 rounded-md border bg-muted/20 p-2.5 sm:p-3"
              >
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm font-medium">
                      {groupLabel} {groupIndex + 1}
                    </span>
                    <Badge variant="secondary">
                      {group.targets.length} 个成员
                    </Badge>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      type="button"
                      size="icon"
                      variant="outline"
                      className="h-8 w-8"
                      aria-label={`添加${groupLabel} ${groupIndex + 1} 的成员`}
                      title="添加成员"
                      onClick={() => addMember(groupIndex)}
                    >
                      <Plus className="h-4 w-4" />
                    </Button>
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      className="h-8 w-8"
                      aria-label={isCollapsed ? `展开${groupLabel} ${groupIndex + 1}` : `折叠${groupLabel} ${groupIndex + 1}`}
                      title={isCollapsed ? '展开' : '折叠'}
                      onClick={() => toggleGroupCollapsed(groupIndex)}
                    >
                      {isCollapsed ? (
                        <ChevronDown className="h-4 w-4" />
                      ) : (
                        <ChevronUp className="h-4 w-4" />
                      )}
                    </Button>
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      className="h-8 w-8"
                      aria-label={`删除${groupLabel} ${groupIndex + 1}`}
                      title={`删除${groupLabel} ${groupIndex + 1}`}
                      onClick={() => removeGroup(groupIndex)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>

                {!isCollapsed && (group.targets.length === 0 ? (
                  <div className="rounded-md bg-background/70 px-3 py-4 text-sm text-muted-foreground">
                    这个{groupLabel}还没有成员。
                  </div>
                ) : (
                  <div className="space-y-1.5">
                    {group.targets.map((member, memberIndex) => (
                      <div
                        key={`${groupIndex}-${memberIndex}`}
                        className="grid min-w-0 items-end gap-2 rounded-md bg-background/80 px-2.5 py-2 md:grid-cols-[minmax(0,5.5rem)_minmax(0,8rem)_minmax(0,6.5rem)_2.25rem] lg:grid-cols-[minmax(0,6rem)_minmax(0,9rem)_minmax(0,7rem)_2.25rem]"
                      >
                        <div className="min-w-0 space-y-0.5">
                          <Label className="text-[11px] leading-none text-muted-foreground">平台</Label>
                          <Input
                            className="h-8 min-w-0"
                            value={member.platform}
                            placeholder="qq"
                            onChange={(event) =>
                              updateMember(groupIndex, memberIndex, {
                                platform: event.target.value,
                              })
                            }
                          />
                        </div>
                        <div className="min-w-0 space-y-0.5">
                          <Label className="text-[11px] leading-none text-muted-foreground">账号 / 群号</Label>
                          <Input
                            className="h-8 min-w-0 font-mono"
                            value={member.item_id}
                            placeholder="123456"
                            onChange={(event) =>
                              updateMember(groupIndex, memberIndex, {
                                item_id: event.target.value,
                              })
                            }
                          />
                        </div>
                        <div className="min-w-0 space-y-0.5">
                          <Label className="text-[11px] leading-none text-muted-foreground">类型</Label>
                          <Select
                            value={member.rule_type}
                            onValueChange={(nextRuleType) =>
                              updateMember(groupIndex, memberIndex, {
                                rule_type: normalizeExpressionRuleType(nextRuleType),
                              })
                            }
                          >
                            <SelectTrigger className="h-8 min-w-0">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="group">群聊</SelectItem>
                              <SelectItem value="private">私聊</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div className="flex items-end justify-between gap-2 md:justify-end">
                          <span className="min-w-0 truncate text-xs text-muted-foreground md:hidden">
                            {formatExpressionTarget(member)}
                          </span>
                          <Button
                            type="button"
                            size="icon"
                            variant="ghost"
                            className="h-8 w-8"
                            aria-label={`删除${groupLabel} ${groupIndex + 1} 的成员 ${memberIndex + 1}`}
                            onClick={() => removeMember(groupIndex, memberIndex)}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

export const JargonGroupsHook = ExpressionGroupsHook

export const BehaviorGroupsHook = ExpressionGroupsHook

export const BehaviorFocusGroupsHook = ExpressionGroupsHook

export const AMemorixSharedMemoryGroupsHook = ExpressionGroupsHook

export const MCPRootItemsHook = createJsonFieldHook({
  emptyValue: [],
  helperText: 'MCP Roots 条目为对象数组，使用 JSON 编辑。',
  placeholder: '[\n  {\n    "enabled": true,\n    "uri": "file:///Users/example/project",\n    "name": "project-root"\n  }\n]',
})

export const MCPServersHook = createJsonFieldHook({
  emptyValue: [],
  helperText: 'MCP 服务器配置结构较复杂，使用 JSON 编辑。',
  placeholder: '[\n  {\n    "name": "example-server",\n    "enabled": true,\n    "transport": "stdio",\n    "command": "uvx",\n    "args": ["example-server"],\n    "env": {},\n    "url": "",\n    "headers": {},\n    "http_timeout_seconds": 30.0,\n    "read_timeout_seconds": 300.0,\n    "authorization": {\n      "mode": "none",\n      "bearer_token": ""\n    }\n  }\n]',
})
