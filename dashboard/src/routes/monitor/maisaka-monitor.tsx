/**
 * MaiSaka 聊天流实时监控组件
 *
 * 通过 WebSocket 实时接收 MaiSaka 推理引擎事件，
 * 以时间线形式展示聊天流的推理过程。
 */
import {
  Activity,
  AlertCircle,
  ArrowRight,
  Bot,
  Brain,
  CheckCircle2,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  CircleDot,
  Clock,
  Eraser,
  ExternalLink,
  Gauge,
  MessageSquare,
  PauseCircle,
  Radio,
  Timer,
  Wrench,
  XCircle,
  Zap,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardHeader, CardTitle } from '@/components/ui/card'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Separator } from '@/components/ui/separator'
import { cn } from '@/lib/utils'
import { useCallback, useEffect, useRef, useState } from 'react'

import type {
  CycleEndEvent,
  CycleStartEvent,
  MaisakaToolCall,
  MessageIngestedEvent,
  MessageSentEvent,
  PlannerFinalizedEvent,
  PlannerResponseEvent,
  ReplierResponseEvent,
  TimingGateResultEvent,
  ToolExecutionEvent,
} from '@/lib/maisaka-monitor-client'
import type { SessionInfo, StageStatusInfo, TimelineEntry } from './use-maisaka-monitor'
import { useMaisakaMonitor } from './use-maisaka-monitor'

// ─── 工具函数 ──────────────────────────────────────────────────

function formatMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

function buildCycleKey(sessionId: string, cycleId: number) {
  return `${sessionId}:${cycleId}`
}

function formatTimestamp(ts: number): string {
  return new Date(ts * 1000).toLocaleTimeString('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function formatRelativeTime(ts: number): string {
  const diff = Date.now() / 1000 - ts
  if (diff < 10) return '刚刚'
  if (diff < 60) return `${Math.round(diff)}秒前`
  if (diff < 3600) return `${Math.round(diff / 60)}分钟前`
  return `${Math.round(diff / 3600)}小时前`
}

// ─── 会话侧边栏 ──────────────────────────────────────────────

function SessionSidebar({
  sessions,
  stageStatuses,
  selectedSession,
  onSelect,
  collapsed,
}: {
  sessions: Map<string, SessionInfo>
  stageStatuses: Map<string, StageStatusInfo>
  selectedSession: string | null
  onSelect: (id: string) => void
  collapsed: boolean
}) {
  const sortedSessions = Array.from(sessions.values()).sort(
    (a, b) => b.lastActivity - a.lastActivity,
  )
  const getSessionInitial = (session: SessionInfo) => {
    const name = session.sessionName.trim()
    if (name) return name.slice(0, 1)
    return session.isGroupChat ? '群' : '私'
  }

  if (sortedSessions.length === 0) {
    if (collapsed) {
      return <div className="h-full p-2" />
    }

    return (
      <div className={cn(
        'flex flex-col items-center justify-center h-full text-muted-foreground gap-2',
        'p-4',
      )}>
        <Bot className="h-8 w-8 opacity-40" />
        <p className="text-sm text-center">等待 MaiSaka 会话…</p>
      </div>
    )
  }

  return (
    <div className={cn('flex flex-col gap-1', collapsed ? 'items-center p-2' : 'p-2')}>
      {sortedSessions.map((session) => {
        const status = stageStatuses.get(session.sessionId)
        return (
        <button
          key={session.sessionId}
          onClick={() => onSelect(session.sessionId)}
          title={session.sessionName}
          className={cn(
            'max-w-full overflow-hidden rounded-lg text-left text-sm transition-colors',
            'hover:bg-accent/50',
            collapsed
              ? 'flex h-10 w-10 items-center justify-center p-0'
              : 'flex w-full min-w-0 flex-col items-start gap-0.5 px-2.5 py-2',
            selectedSession === session.sessionId && 'bg-accent text-accent-foreground',
          )}
        >
          <div className={cn('flex w-full min-w-0 items-center', collapsed ? 'justify-center' : 'justify-between gap-2')}>
            <div className={cn('flex min-w-0 items-center gap-2 overflow-hidden', !collapsed && 'flex-1')}>
              <span className="relative flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-primary/10 text-xs font-semibold text-primary">
                {getSessionInitial(session)}
                {status && (
                  <span className="absolute -right-0.5 -top-0.5 h-2.5 w-2.5 rounded-full bg-emerald-500 ring-2 ring-background" />
                )}
              </span>
              {!collapsed && <span className="block min-w-0 flex-1 overflow-hidden text-ellipsis whitespace-nowrap font-medium" title={session.sessionName}>
                {session.sessionName}
              </span>}
            </div>
            {!collapsed && <Badge variant="secondary" className="h-4 shrink-0 px-1 text-[10px]">
              {session.eventCount}
            </Badge>}
          </div>
          {!collapsed && (
            <div className="flex w-full min-w-0 items-center justify-between gap-2 overflow-hidden text-xs text-muted-foreground">
              <span className="shrink-0">{formatRelativeTime(session.lastActivity)}</span>
              {status && <span className="min-w-0 truncate text-primary">{status.stage}</span>}
            </div>
          )}
        </button>
        )
      })}
    </div>
  )
}

// ─── 单条时间线事件渲染 ──────────────────────────────────────

function StageStatusPanel({ status }: { status?: StageStatusInfo }) {
  if (!status) {
    return (
      <div className="mb-3 rounded-md border bg-muted/30 px-3 py-2 text-sm text-muted-foreground">
        当前聊天流暂无阶段状态
      </div>
    )
  }

  return (
    <div className="mb-3 rounded-md border bg-background px-3 py-2">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="default" className="gap-1">
          <Activity className="h-3 w-3" />
          {status.stage || '未知阶段'}
        </Badge>
        {status.roundText && (
          <Badge variant="secondary" className="text-[10px]">
            {status.roundText}
          </Badge>
        )}
        {status.agentState && (
          <Badge variant={status.agentState === 'running' ? 'default' : 'outline'} className="text-[10px]">
            {status.agentState}
          </Badge>
        )}
        <span className="ml-auto text-xs text-muted-foreground">
          更新于 {formatRelativeTime(status.updatedAt)}
        </span>
      </div>
      {status.detail && (
        <p className="mt-1 text-sm text-muted-foreground">{status.detail}</p>
      )}
    </div>
  )
}

function MessageIngestedCard({ data }: { data: MessageIngestedEvent }) {
  return (
    <div className="flex items-start gap-3">
      <div className="mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-blue-500/15 text-blue-500">
        <MessageSquare className="h-3.5 w-3.5" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="font-medium text-sm">{data.speaker_name}</span>
          <span className="text-xs text-muted-foreground">{formatTimestamp(data.timestamp)}</span>
        </div>
        <p className="text-sm text-foreground/80 whitespace-pre-wrap wrap-break-word leading-relaxed">
          {data.content || '[空消息]'}
        </p>
      </div>
    </div>
  )
}

function MessageSentCard({ data }: { data: MessageSentEvent }) {
  return (
    <div className="flex items-start gap-3 rounded-md border border-emerald-500/30 bg-emerald-500/5 px-3 py-2">
      <div className="mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-emerald-500/15 text-emerald-500">
        <Bot className="h-3.5 w-3.5" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="mb-1 flex items-center gap-2">
          <span className="font-medium text-sm">{data.speaker_name || '麦麦'}</span>
          <Badge variant="outline" className="text-[10px]">已发送</Badge>
          <span className="text-xs text-muted-foreground">{formatTimestamp(data.timestamp)}</span>
        </div>
        <p className="text-sm text-foreground/80 whitespace-pre-wrap wrap-break-word leading-relaxed">
          {data.content || '[非文本消息]'}
        </p>
      </div>
    </div>
  )
}

function CycleStartCard({ data }: { data: CycleStartEvent }) {
  return (
    <div className="flex items-center gap-3">
      <div className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-violet-500/15 text-violet-500">
        <Zap className="h-3.5 w-3.5" />
      </div>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-sm font-medium">推理循环 #{data.cycle_id}</span>
        <Badge variant="outline" className="text-[10px]">
          回合 {data.round_index + 1}/{data.max_rounds}
        </Badge>
        <Badge variant="secondary" className="text-[10px]">
          上下文 {data.history_count} 条
        </Badge>
      </div>
    </div>
  )
}

function TimingGateCard({ data }: { data: TimingGateResultEvent }) {
  const actionConfig: Record<string, { label: string; variant: 'default' | 'secondary' | 'destructive'; icon: typeof ArrowRight }> = {
    continue: { label: '继续执行', variant: 'default', icon: ArrowRight },
    wait: { label: '等待', variant: 'secondary', icon: PauseCircle },
    no_action: { label: '不回复', variant: 'destructive', icon: XCircle },
  }
  const config = actionConfig[data.action] ?? actionConfig.continue
  const Icon = config.icon

  return (
    <div className="flex items-start gap-3 rounded-md border bg-background px-3 py-2 shadow-sm">
      <div className="mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-amber-500/15 text-amber-500">
        <Timer className="h-3.5 w-3.5" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1 flex-wrap">
          <span className="text-sm font-medium">反应</span>
          <Badge variant="outline" className="text-[10px]">react</Badge>
          <Badge variant={config.variant} className="text-[10px] gap-0.5">
            <Icon className="h-2.5 w-2.5" />
            {config.label}
          </Badge>
          <span className="text-xs text-muted-foreground">{formatMs(data.duration_ms)}</span>
        </div>
        {data.content && (
          <CollapsibleText text={data.content} maxLines={3} />
        )}
      </div>
    </div>
  )
}

function ToolCallBadges({ toolCalls }: { toolCalls: MaisakaToolCall[] }) {
  if (toolCalls.length <= 0) {
    return null
  }

  return (
    <div className="mt-2 flex flex-wrap gap-1.5">
      {toolCalls.map((tc: MaisakaToolCall, idx: number) => (
        <Badge key={`${tc.id || tc.name}-${idx}`} variant="secondary" className="text-[10px] gap-1">
          <Wrench className="h-2.5 w-2.5" />
          {tc.name}
        </Badge>
      ))}
    </div>
  )
}

function openPromptHtml(uri: string) {
  const normalized = uri.trim()
  if (!normalized) return
  window.open(normalized, '_blank', 'noopener,noreferrer')
}

function isPlannerInterrupted(data: PlannerFinalizedEvent) {
  const content = data.planner?.content?.trim() ?? ''
  return data.interrupted === true || (
    content.startsWith('Planner ') &&
    data.planner?.prompt_tokens === 0 &&
    data.planner?.completion_tokens === 0 &&
    data.planner?.tool_calls.length === 0
  )
}

function PlannerInterruptedCard({ data }: { data: PlannerFinalizedEvent }) {
  const planner = data.planner

  return (
    <div className="rounded-md border border-amber-500/35 bg-amber-500/5 px-3 py-2">
      <div className="flex items-center gap-2 text-sm">
        <AlertCircle className="h-4 w-4 shrink-0 text-amber-500" />
        <span className="font-medium">Planner 被新消息打断</span>
        <Badge variant="outline" className="ml-auto text-[10px]">
          #{data.cycle_id}
        </Badge>
        {planner && planner.duration_ms > 0 && (
          <span className="text-xs text-muted-foreground">{formatMs(planner.duration_ms)}</span>
        )}
      </div>
      <p className="mt-1 text-sm text-muted-foreground">
        {planner?.content || '收到新消息，已停止当前思考并准备重新决策。'}
      </p>
    </div>
  )
}

function PlannerResponseCard({ data }: { data: PlannerResponseEvent }) {
  return (
    <div className="flex items-start gap-3">
      <div className="mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-emerald-500/15 text-emerald-500">
        <Brain className="h-3.5 w-3.5" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1 flex-wrap">
          <span className="text-sm font-medium">规划器思考</span>
          <span className="text-xs text-muted-foreground">{formatMs(data.duration_ms)}</span>
          <Badge variant="outline" className="text-[10px]">
            {data.prompt_tokens}+{data.completion_tokens} tokens
          </Badge>
        </div>
        {data.content && (
          <CollapsibleText text={data.content} maxLines={6} />
        )}
        <ToolCallBadges toolCalls={data.tool_calls} />
      </div>
    </div>
  )
}

function PlannerFinalizedCard({ data }: { data: PlannerFinalizedEvent }) {
  const planner = data.planner
  const promptHtmlUri = planner?.prompt_html_uri?.trim() ?? ''

  return (
    <Card className="border-l-4 border-l-emerald-500/60">
      <CardHeader className="py-3 px-4 space-y-3">
        <div className="flex items-center gap-2 flex-wrap">
          <Brain className="h-4 w-4 text-emerald-500" />
          <CardTitle className="text-sm font-medium">主循环 planner</CardTitle>
          {promptHtmlUri && (
            <Button
              variant="ghost"
              size="sm"
              className="h-6 px-2 text-[10px]"
              onClick={() => openPromptHtml(promptHtmlUri)}
              title="打开 planner HTML 记录"
            >
              <ExternalLink className="mr-1 h-3 w-3" />
              HTML
            </Button>
          )}
          <Badge variant="outline" className="text-xs font-normal ml-auto">
            {formatMs(planner?.duration_ms ?? 0)}
          </Badge>
          {data.request && (
            <Badge variant="secondary" className="text-[10px]">
              上下文 {data.request.selected_history_count} 条 / 可用工具 {data.request.tool_count}
            </Badge>
          )}
          {planner && (planner.prompt_tokens > 0 || planner.completion_tokens > 0) && (
            <Badge variant="outline" className="text-[10px]">
              {planner.prompt_tokens}+{planner.completion_tokens} tokens
            </Badge>
          )}
        </div>

        {planner?.content ? (
          <CollapsibleText text={planner.content} maxLines={6} className="text-foreground/90" />
        ) : (
          <p className="text-sm text-muted-foreground">planner 本轮没有文本内容</p>
        )}

      </CardHeader>
    </Card>
  )
}

function PlannerToolCallsBlock({ data }: { data: PlannerFinalizedEvent }) {
  const toolCalls = data.planner?.tool_calls ?? []
  const tools = data.tools ?? []
  const displayTools = tools.length > 0
    ? tools
    : toolCalls.map((toolCall) => ({
        tool_call_id: toolCall.id,
        tool_name: toolCall.name,
        tool_args: toolCall.arguments ?? {},
        success: true,
        duration_ms: 0,
        summary: '',
      }))
  const isFinishTool = (toolName?: string) => toolName?.trim().toLowerCase() === 'finish'
  const finishTools = displayTools.filter((tool) => isFinishTool(tool.tool_name))
  const regularTools = displayTools.filter((tool) => !isFinishTool(tool.tool_name))

  if (displayTools.length <= 0) {
    return null
  }

  if (regularTools.length <= 0 && finishTools.length > 0) {
    return (
      <div className="rounded-md border border-emerald-500/30 bg-emerald-500/5 px-3 py-2">
        <div className="flex items-center gap-2 text-sm">
          <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-500" />
          <span className="font-medium">本轮思考暂时结束</span>
          <span className="text-muted-foreground">等待新的消息。</span>
        </div>
      </div>
    )
  }

  return (
    <Card className="border-l-4 border-l-teal-500/60">
      <CardHeader className="py-3 px-4 space-y-2">
        <div className="flex items-center gap-2">
          <Wrench className="h-4 w-4 text-teal-500" />
          <CardTitle className="text-sm font-medium">Planner 工具调用</CardTitle>
          <Badge variant="secondary" className="ml-auto text-[10px]">
            {regularTools.length} 个
          </Badge>
        </div>
        {finishTools.length > 0 && (
          <div className="flex items-center gap-2 rounded-md border border-emerald-500/30 bg-emerald-500/5 px-2.5 py-1.5 text-xs">
            <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-emerald-500" />
            <span className="font-medium">本轮思考暂时结束</span>
            <span className="text-muted-foreground">等待新的消息。</span>
          </div>
        )}
        <div className="space-y-2">
          {regularTools.map((tool, idx) => (
            <div
              key={`${tool.tool_call_id || tool.tool_name}-${idx}`}
              className="rounded-md border bg-muted/40 px-2.5 py-2 text-xs"
            >
              <div className="flex items-center gap-2">
                <span className="font-mono font-medium">{tool.tool_name || 'unknown'}</span>
                {tool.success
                  ? <CheckCircle2 className="h-3.5 w-3.5 text-teal-500" />
                  : <XCircle className="h-3.5 w-3.5 text-red-500" />
                }
                {tool.duration_ms > 0 && (
                  <span className="text-muted-foreground">{formatMs(tool.duration_ms)}</span>
                )}
              </div>
              {Object.keys(tool.tool_args ?? {}).length > 0 && (
                <pre className="mt-1 whitespace-pre-wrap break-all rounded bg-background/70 px-2 py-1 text-[11px] text-muted-foreground">
                  {JSON.stringify(tool.tool_args, null, 2)}
                </pre>
              )}
              {tool.summary && (
                <p className="mt-1 text-muted-foreground whitespace-pre-wrap break-words">{tool.summary}</p>
              )}
            </div>
          ))}
        </div>
      </CardHeader>
    </Card>
  )
}

function ToolExecutionCard({ data }: { data: ToolExecutionEvent }) {
  return (
    <div className="flex items-start gap-3">
      <div className={cn(
        'mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full',
        data.success
          ? 'bg-teal-500/15 text-teal-500'
          : 'bg-red-500/15 text-red-500',
      )}>
        <Wrench className="h-3.5 w-3.5" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1 flex-wrap">
          <span className="text-sm font-medium font-mono">{data.tool_name}</span>
          {data.success
            ? <CheckCircle2 className="h-3.5 w-3.5 text-teal-500" />
            : <XCircle className="h-3.5 w-3.5 text-red-500" />
          }
          <span className="text-xs text-muted-foreground">{formatMs(data.duration_ms)}</span>
        </div>
        {Object.keys(data.tool_args).length > 0 && (
          <div className="text-xs text-muted-foreground font-mono bg-muted/50 rounded px-2 py-1 mb-1 whitespace-pre-wrap break-all">
            {JSON.stringify(data.tool_args, null, 2)}
          </div>
        )}
        {data.result_summary && (
          <CollapsibleText text={data.result_summary} maxLines={3} className="text-muted-foreground" />
        )}
      </div>
    </div>
  )
}

function getCycleEndReasonText(data: CycleEndEvent) {
  const reason = data.end_reason ?? ''
  const detail = data.end_detail?.trim()

  if (detail) {
    return detail
  }

  if (reason === 'finish') return 'Planner 调用 finish，结束本轮思考并等待新消息。'
  if (reason === 'timing_no_action') return 'Timing Gate 选择 no_action，本轮不会进入 Planner。'
  if (reason === 'max_rounds') return '已达到内部思考轮次上限，本轮处理结束。'
  if (reason === 'planner_interrupted') return 'Planner 被新消息打断，当前轮结束。'
  if (reason.startsWith('tool_pause:')) return `工具 ${reason.slice('tool_pause:'.length)} 要求暂停当前思考循环。`
  if (reason === 'tool_pause') return '工具要求暂停当前思考循环。'
  if (reason === 'empty_planner_response') return 'Planner 没有返回文本或工具调用，本轮思考结束。'
  if (reason === 'tool_continue') return 'Planner 工具执行完成，继续下一轮内部思考。'
  return '本轮思考完成。'
}

function getCycleEndReasonLabel(data: CycleEndEvent) {
  const reason = data.end_reason ?? ''

  if (reason === 'finish') return 'finish 结束'
  if (reason === 'timing_no_action') return 'no_action 结束'
  if (reason === 'max_rounds') return '轮次上限'
  if (reason === 'planner_interrupted') return 'Planner 打断'
  if (reason.startsWith('tool_pause:')) return '工具暂停'
  if (reason === 'tool_pause') return '工具暂停'
  if (reason === 'empty_planner_response') return '空响应'
  if (reason === 'tool_continue') return '继续下一轮'
  return '循环结束'
}

function CycleEndCard({ data }: { data: CycleEndEvent }) {
  const totalTime = Object.values(data.time_records).reduce((a, b) => a + b, 0)
  return (
    <div className="my-1 space-y-1.5">
      <div className="flex items-center gap-3">
        <Separator className="flex-1" />
        <div className="flex items-center gap-2 rounded-full border bg-background px-3 py-1">
          <CircleDot className="h-3.5 w-3.5 text-slate-500" />
          <span className="text-xs text-muted-foreground">{getCycleEndReasonLabel(data)}</span>
          <Badge variant="outline" className="text-[10px]">
            #{data.cycle_id}
          </Badge>
          <span className="text-[10px] text-muted-foreground">{formatMs(totalTime * 1000)}</span>
          <Badge
            variant={data.agent_state === 'running' ? 'default' : 'secondary'}
            className="text-[10px]"
          >
            {data.agent_state}
          </Badge>
        </div>
        <Separator className="flex-1" />
      </div>
      <p className="text-center text-xs text-muted-foreground">{getCycleEndReasonText(data)}</p>
    </div>
  )
}

// ─── 可折叠文本组件 ────────────────────────────────────────────

function CollapsibleText({
  text,
  maxLines = 4,
  className,
}: {
  text: string
  maxLines?: number
  className?: string
}) {
  const [expanded, setExpanded] = useState(false)
  const lines = text.split('\n')
  const needsCollapse = lines.length > maxLines

  if (!needsCollapse || expanded) {
    return (
      <div className="relative">
        <p className={cn(
          'text-sm whitespace-pre-wrap wrap-break-word leading-relaxed',
          className,
        )}>
          {text}
        </p>
        {needsCollapse && (
          <button
            onClick={() => setExpanded(false)}
            className="text-xs text-primary hover:underline mt-1 flex items-center gap-0.5"
          >
            <ChevronDown className="h-3 w-3" /> 收起
          </button>
        )}
      </div>
    )
  }

  return (
    <div>
      <p className={cn(
        'text-sm whitespace-pre-wrap wrap-break-word leading-relaxed',
        className,
      )}>
        {lines.slice(0, maxLines).join('\n')}
      </p>
      <button
        onClick={() => setExpanded(true)}
        className="text-xs text-primary hover:underline mt-1 flex items-center gap-0.5"
      >
        <ChevronRight className="h-3 w-3" /> 展开全部 ({lines.length} 行)
      </button>
    </div>
  )
}

// ─── 回复器响应卡片 ──────────────────────────────────────────

function ReplierResponseCard({ data }: { data: ReplierResponseEvent }) {
  return (
    <Card className="border-l-4 border-l-purple-500/60">
      <CardHeader className="py-2.5 px-4 space-y-2">
        <div className="flex items-center gap-2">
          <Bot className="h-4 w-4 text-purple-500" />
          <CardTitle className="text-sm font-medium">回复器响应</CardTitle>
          <Badge variant="outline" className="text-xs font-normal ml-auto">
            {formatMs(data.duration_ms)}
          </Badge>
          {data.success ? (
            <Badge variant="secondary" className="text-xs gap-1">
              <CheckCircle2 className="h-3 w-3" /> 成功
            </Badge>
          ) : (
            <Badge variant="destructive" className="text-xs gap-1">
              <XCircle className="h-3 w-3" /> 失败
            </Badge>
          )}
          <span className="text-xs text-muted-foreground">{formatTimestamp(data.timestamp)}</span>
        </div>
        {data.content && (
          <CollapsibleText text={data.content} maxLines={6} className="text-foreground/90" />
        )}
        {data.reasoning && (
          <details className="mt-1">
            <summary className="text-xs text-muted-foreground cursor-pointer hover:text-foreground">
              思考过程
            </summary>
            <CollapsibleText text={data.reasoning} maxLines={8} className="mt-1 text-muted-foreground" />
          </details>
        )}
        {(data.prompt_tokens > 0 || data.completion_tokens > 0) && (
          <div className="flex gap-3 text-xs text-muted-foreground mt-1">
            {data.model_name && <span>模型: {data.model_name}</span>}
            <span>输入: {data.prompt_tokens}</span>
            <span>输出: {data.completion_tokens}</span>
            <span>总计: {data.total_tokens}</span>
          </div>
        )}
      </CardHeader>
    </Card>
  )
}

// ─── 时间线入口渲染器 ──────────────────────────────────────────

function TimelineEventRenderer({
  entry,
  showCycleMarkers,
}: {
  entry: TimelineEntry
  showCycleMarkers: boolean
}) {
  switch (entry.type) {
    case 'message.ingested':
      return <MessageIngestedCard data={entry.data as MessageIngestedEvent} />
    case 'message.sent':
      return <MessageSentCard data={entry.data as MessageSentEvent} />
    case 'cycle.start':
      if (!showCycleMarkers) return null
      return <CycleStartCard data={entry.data as CycleStartEvent} />
    case 'timing_gate.result':
      return <TimingGateCard data={entry.data as TimingGateResultEvent} />
    case 'planner.response':
      return <PlannerResponseCard data={entry.data as PlannerResponseEvent} />
    case 'planner.finalized':
      if (isPlannerInterrupted(entry.data as PlannerFinalizedEvent)) {
        return <PlannerInterruptedCard data={entry.data as PlannerFinalizedEvent} />
      }
      if ((entry.data as PlannerFinalizedEvent).timing_gate?.result?.action === 'no_action') {
        return null
      }
      return (
        <div className="space-y-2">
          <PlannerFinalizedCard data={entry.data as PlannerFinalizedEvent} />
          <PlannerToolCallsBlock data={entry.data as PlannerFinalizedEvent} />
        </div>
      )
    case 'tool.execution':
      return <ToolExecutionCard data={entry.data as ToolExecutionEvent} />
    case 'cycle.end':
      return <CycleEndCard data={entry.data as CycleEndEvent} />
    case 'replier.response':
      return <ReplierResponseCard data={entry.data as ReplierResponseEvent} />
    // planner.request, replier.request 和 session.start 通常不需要在 timeline 中主要展示
    default:
      return null
  }
}

// ─── 主组件 ─────────────────────────────────────────────────

export function MaisakaMonitor() {
  const {
    timeline,
    sessions,
    stageStatuses,
    selectedSession,
    setSelectedSession,
    connected,
    backgroundCollection,
    setBackgroundCollectionEnabled,
    clearTimeline,
  } = useMaisakaMonitor()

  const scrollRef = useRef<HTMLDivElement>(null)
  const [autoScroll, setAutoScroll] = useState(true)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    const saved = localStorage.getItem('maisaka-monitor-sidebar-collapsed')
    return saved !== 'false'
  })
  const [showCycleMarkers, setShowCycleMarkers] = useState(() => {
    const saved = localStorage.getItem('maisaka-monitor-show-cycle-markers')
    return saved === 'true'
  })

  useEffect(() => {
    localStorage.setItem('maisaka-monitor-sidebar-collapsed', String(sidebarCollapsed))
  }, [sidebarCollapsed])

  useEffect(() => {
    localStorage.setItem('maisaka-monitor-show-cycle-markers', String(showCycleMarkers))
  }, [showCycleMarkers])

  // 自动滚动到底部
  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      const viewport = scrollRef.current.querySelector('[data-radix-scroll-area-viewport]')
      if (viewport) {
        viewport.scrollTop = viewport.scrollHeight
      }
    }
  }, [timeline, autoScroll])

  const handleScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    const target = e.currentTarget.querySelector('[data-radix-scroll-area-viewport]')
    if (!target) return
    const { scrollTop, scrollHeight, clientHeight } = target as HTMLElement
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 80)
  }, [])

  // 统计当前会话的各事件类型计数
  const stats = {
    messages: timeline.filter((e) => e.type === 'message.ingested' || e.type === 'message.sent').length,
    cycles: timeline.filter((e) => e.type === 'cycle.start').length,
    toolCalls: timeline.reduce((count, entry) => {
      if (entry.type === 'tool.execution') {
        return count + 1
      }
      if (entry.type === 'planner.finalized') {
        return count + ((entry.data as PlannerFinalizedEvent).tools?.length ?? 0)
      }
      return count
    }, 0),
  }
  const selectedStageStatus = selectedSession ? stageStatuses.get(selectedSession) : undefined

  return (
    <div className="flex min-w-0 flex-col gap-4 lg:h-[calc(100vh-180px)] lg:flex-row">
      {/* 会话侧边栏 */}
      <Card className={cn(
        'flex min-w-0 shrink-0 flex-col transition-[width] duration-200',
        sidebarCollapsed ? 'w-full lg:w-16' : 'w-full lg:w-52',
      )}>
        <CardHeader className={cn('py-3 space-y-0', sidebarCollapsed ? 'px-2' : 'px-3')}>
          <CardTitle className={cn(
            'text-sm font-medium flex items-center gap-2',
            sidebarCollapsed && 'justify-center text-[0px]',
          )}>
            {!sidebarCollapsed && <Activity className="h-4 w-4" />}
            聊天流
            {connected && (
              <span className={cn('flex h-2 w-2 rounded-full bg-emerald-500', !sidebarCollapsed && 'ml-auto')} />
            )}
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6 shrink-0"
              onClick={() => setSidebarCollapsed((value) => !value)}
              title={sidebarCollapsed ? '展开侧边栏' : '折叠侧边栏'}
            >
              {sidebarCollapsed ? <ChevronRight className="h-3.5 w-3.5" /> : <ChevronLeft className="h-3.5 w-3.5" />}
            </Button>
          </CardTitle>
        </CardHeader>
        <Separator />
        <ScrollArea className="max-h-40 flex-1 lg:max-h-none">
          <SessionSidebar
            sessions={sessions}
            stageStatuses={stageStatuses}
            selectedSession={selectedSession}
            onSelect={setSelectedSession}
            collapsed={sidebarCollapsed}
          />
        </ScrollArea>
      </Card>

      {/* 主时间线区域 */}
      <div className="flex min-w-0 flex-1 flex-col">
        {/* 顶部统计栏 */}
        <div className="mb-3 flex min-w-0 flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center">
          <div className="flex min-w-0 flex-wrap items-center gap-x-4 gap-y-2 text-sm">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <MessageSquare className="h-3.5 w-3.5" />
              <span>{stats.messages} 消息</span>
            </div>
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Brain className="h-3.5 w-3.5" />
              <span>{stats.cycles} 循环</span>
            </div>
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Wrench className="h-3.5 w-3.5" />
              <span>{stats.toolCalls} 工具调用</span>
            </div>
          </div>
          <div className="flex min-w-0 flex-wrap items-center gap-2 sm:ml-auto">
            <Button
              variant={backgroundCollection ? 'secondary' : 'ghost'}
              size="sm"
              className="h-7 max-w-full text-xs"
              onClick={() => setBackgroundCollectionEnabled(!backgroundCollection)}
              title={backgroundCollection ? '关闭离开页面后的持续获取' : '开启离开页面后的持续获取'}
            >
              <Radio className={cn('h-3.5 w-3.5 mr-1', backgroundCollection && 'text-primary')} />
              持续获取
            </Button>
            <Button
              variant={showCycleMarkers ? 'secondary' : 'ghost'}
              size="sm"
              className="h-7 max-w-full text-xs"
              onClick={() => setShowCycleMarkers((value) => !value)}
              title={showCycleMarkers ? '隐藏推理循环标记' : '显示推理循环标记'}
            >
              <CircleDot className={cn('h-3.5 w-3.5 mr-1', showCycleMarkers && 'text-primary')} />
              循环标记
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className="h-7 max-w-full text-xs"
              onClick={() => setAutoScroll(!autoScroll)}
            >
              <Gauge className={cn('h-3.5 w-3.5 mr-1', autoScroll && 'text-primary')} />
              {autoScroll ? '跟踪中' : '已暂停'}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className="h-7 max-w-full text-xs"
              onClick={clearTimeline}
            >
              <Eraser className="h-3.5 w-3.5 mr-1" />
              清空
            </Button>
          </div>
        </div>

        {/* 时间线 */}
        <StageStatusPanel status={selectedStageStatus} />

        <Card className="min-h-[420px] min-w-0 flex-1 overflow-hidden lg:min-h-0">
          <ScrollArea
            className="h-full"
            ref={scrollRef}
            onScrollCapture={handleScroll}
          >
            <div className="min-w-0 space-y-3 p-4">
              {timeline.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-20 text-muted-foreground gap-3">
                  <Clock className="h-10 w-10 opacity-30" />
                  <p className="text-sm">等待 MaiSaka 推理事件…</p>
                  <p className="text-xs opacity-60">
                    当 MaiSaka 处理新消息时，推理过程会实时展示在这里
                  </p>
                </div>
              ) : (
                (() => {
                  const noReplyTimingGateCycles = new Set<string>()

                  return timeline.map((entry) => {
                    if (entry.type === 'timing_gate.result') {
                      const data = entry.data as TimingGateResultEvent
                      if (data.action === 'no_action') {
                        noReplyTimingGateCycles.add(buildCycleKey(data.session_id, data.cycle_id))
                      }
                    }

                    if (entry.type === 'planner.response' || entry.type === 'planner.finalized') {
                      const data = entry.data as PlannerResponseEvent | PlannerFinalizedEvent
                      const cycleKey = buildCycleKey(data.session_id, data.cycle_id)
                      if (entry.type === 'planner.finalized' && isPlannerInterrupted(data as PlannerFinalizedEvent)) {
                        const rendered = <TimelineEventRenderer entry={entry} showCycleMarkers={showCycleMarkers} />
                        if (!rendered) return null
                        return (
                          <div
                            key={entry.id}
                            className="animate-in fade-in-0 slide-in-from-bottom-2 duration-300"
                          >
                            {rendered}
                          </div>
                        )
                      }
                      if (noReplyTimingGateCycles.has(cycleKey)) {
                        return null
                      }
                    }

                    const rendered = <TimelineEventRenderer entry={entry} showCycleMarkers={showCycleMarkers} />
                    if (!rendered) return null
                    return (
                      <div
                        key={entry.id}
                        className="animate-in fade-in-0 slide-in-from-bottom-2 duration-300"
                      >
                        {rendered}
                      </div>
                    )
                  })
                })()
              )}
            </div>
          </ScrollArea>
        </Card>
      </div>
    </div>
  )
}
