import { memo, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import {
  BrainCircuit,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  GitBranch,
  Loader2,
  RefreshCw,
  Search,
} from 'lucide-react'
import ReactFlow, {
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  MarkerType,
  Position,
  type Edge,
  type Node,
  type NodeProps,
  type NodeTypes,
} from 'reactflow'

import 'reactflow/dist/style.css'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Textarea } from '@/components/ui/textarea'
import { useToast } from '@/hooks/use-toast'
import {
  debugBehaviorRetrieval,
  getBehaviorPathDetail,
  listBehaviorChats,
  listBehaviorPaths,
  type BehaviorChatInfo,
  type BehaviorPathDetail,
  type BehaviorPathItem,
  type BehaviorRetrievalDebugPayload,
} from '@/lib/behavior-api'
import { cn } from '@/lib/utils'

const PAGE_SIZE = 20

type ActiveTab = 'paths' | 'debug' | 'graph'

interface BehaviorSceneGroup {
  key: string
  trigger: string
  chatName: string
  paths: BehaviorPathItem[]
  latestUpdate: string | null
  bestScore: number
  activationCount: number
  successCount: number
  failureCount: number
}

interface BehaviorFlowNodeData {
  label: string
  kind: string
  detail: string
}

type BehaviorFlowNode = Node<BehaviorFlowNodeData>
type BehaviorFlowEdge = Edge

const BehaviorGraphNode = memo(({ data }: NodeProps<BehaviorFlowNodeData>) => {
  const styleByKind: Record<string, string> = {
    action: 'border-emerald-300 bg-emerald-500 text-white shadow-[0_10px_28px_rgba(16,185,129,0.2)]',
    outcome: 'border-sky-300 bg-sky-500 text-white shadow-[0_10px_28px_rgba(14,165,233,0.2)]',
    path: 'border-violet-300 bg-violet-500 text-white shadow-[0_10px_28px_rgba(139,92,246,0.2)]',
  }
  const className =
    styleByKind[data.kind] ??
    'border-slate-300 bg-slate-700 text-white shadow-[0_10px_24px_rgba(15,23,42,0.16)]'

  return (
    <div className={cn('w-56 rounded-lg border px-3 py-2 text-left', className)}>
      <Handle className="opacity-0" type="target" position={Position.Left} />
      <div className="mb-1 text-[11px] font-medium uppercase opacity-75">{data.kind}</div>
      <div className="line-clamp-3 text-xs font-semibold leading-5" title={data.detail}>
        {data.label}
      </div>
      <Handle className="opacity-0" type="source" position={Position.Right} />
    </div>
  )
})

BehaviorGraphNode.displayName = 'BehaviorGraphNode'

const behaviorNodeTypes: NodeTypes = {
  behavior: BehaviorGraphNode,
}

function formatTime(value: string | null): string {
  if (!value) return '-'
  return new Date(value).toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function splitTags(value: string): string[] {
  return value
    .split(/[，,、\n]/)
    .map((item) => item.trim())
    .filter(Boolean)
}

function formatScore(value: number): string {
  return Number.isFinite(value) ? value.toFixed(2) : '0.00'
}

function shortText(value: string, maxLength = 72): string {
  if (value.length <= maxLength) return value
  return `${value.slice(0, maxLength)}...`
}

function parseSceneStart(value: string): {
  summary: string
  intent: string
  phase: string
  domains: string
  needs: string
  raw: string
} {
  const parts = value
    .split(/[;；]/)
    .map((item) => item.trim())
    .filter(Boolean)
  return {
    summary: parts[0] ?? value,
    intent: parts[1] ?? '',
    phase: parts[2] ?? '',
    domains: parts[3] ?? '',
    needs: parts[4] ?? '',
    raw: value,
  }
}

export function BehaviorLearningPage() {
  const { toast } = useToast()
  const [activeTab, setActiveTab] = useState<ActiveTab>('paths')
  const [chats, setChats] = useState<BehaviorChatInfo[]>([])
  const [selectedSessionId, setSelectedSessionId] = useState('all')
  const [search, setSearch] = useState('')
  const [enabledFilter, setEnabledFilter] = useState('all')
  const [paths, setPaths] = useState<BehaviorPathItem[]>([])
  const [openSceneGroups, setOpenSceneGroups] = useState<Set<string>>(new Set())
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(false)
  const [selectedPathId, setSelectedPathId] = useState<number | null>(null)
  const [detail, setDetail] = useState<BehaviorPathDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [debugLoading, setDebugLoading] = useState(false)
  const [debugResult, setDebugResult] = useState<BehaviorRetrievalDebugPayload | null>(null)
  const [debugForm, setDebugForm] = useState({
    summary: '',
    userIntent: '',
    conversationPhase: '',
    domainTags: '',
    behaviorNeeds: '',
    riskFlags: '',
    avoidBehaviors: '',
    retrievalQuery: '',
  })

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const selectedChatName = useMemo(() => {
    if (selectedSessionId === 'all') return '全部聊天流'
    if (selectedSessionId === '__global__') return '全局行为'
    return chats.find((chat) => chat.session_id === selectedSessionId)?.display_name ?? selectedSessionId
  }, [chats, selectedSessionId])
  const sceneGroups = useMemo(() => {
    const groups = new Map<string, BehaviorSceneGroup>()
    paths.forEach((path) => {
      const key = `${path.session_id || '__global__'}::${path.trigger}`
      const existing = groups.get(key)
      if (!existing) {
        groups.set(key, {
          key,
          trigger: path.trigger,
          chatName: path.chat_name,
          paths: [path],
          latestUpdate: path.update_time,
          bestScore: path.score,
          activationCount: path.activation_count,
          successCount: path.success_count,
          failureCount: path.failure_count,
        })
        return
      }
      existing.paths.push(path)
      existing.bestScore = Math.max(existing.bestScore, path.score)
      existing.activationCount += path.activation_count
      existing.successCount += path.success_count
      existing.failureCount += path.failure_count
      if (!existing.latestUpdate || (path.update_time && path.update_time > existing.latestUpdate)) {
        existing.latestUpdate = path.update_time
      }
    })
    return Array.from(groups.values()).sort((left, right) => {
      const leftTime = left.latestUpdate ?? ''
      const rightTime = right.latestUpdate ?? ''
      return rightTime.localeCompare(leftTime)
    })
  }, [paths])

  const loadChats = async () => {
    try {
      const result = await listBehaviorChats()
      if (result.success) setChats(result.data)
    } catch (error) {
      toast({
        title: '加载聊天流失败',
        description: error instanceof Error ? error.message : '无法读取行为学习聊天流',
        variant: 'destructive',
      })
    }
  }

  const loadPaths = async () => {
    try {
      setLoading(true)
      const result = await listBehaviorPaths({
        session_id: selectedSessionId,
        search,
        enabled: enabledFilter,
        page,
        page_size: PAGE_SIZE,
      })
      setPaths(result.data)
      setTotal(result.total)
      if (!selectedPathId && result.data.length > 0) {
        setSelectedPathId(result.data[0].id)
      }
    } catch (error) {
      toast({
        title: '加载行为路径失败',
        description: error instanceof Error ? error.message : '无法读取行为经验路径',
        variant: 'destructive',
      })
    } finally {
      setLoading(false)
    }
  }

  const loadDetail = async (pathId: number) => {
    try {
      setDetailLoading(true)
      const result = await getBehaviorPathDetail(pathId)
      setDetail(result.data)
    } catch (error) {
      toast({
        title: '加载局部图谱失败',
        description: error instanceof Error ? error.message : '无法读取行为路径详情',
        variant: 'destructive',
      })
    } finally {
      setDetailLoading(false)
    }
  }

  const runDebug = async () => {
    try {
      setDebugLoading(true)
      const result = await debugBehaviorRetrieval({
        session_id: selectedSessionId === 'all' || selectedSessionId === '__global__' ? undefined : selectedSessionId,
        include_global: selectedSessionId === 'all',
        summary: debugForm.summary,
        user_intent: debugForm.userIntent,
        conversation_phase: debugForm.conversationPhase,
        domain_tags: splitTags(debugForm.domainTags),
        behavior_needs: splitTags(debugForm.behaviorNeeds),
        risk_flags: splitTags(debugForm.riskFlags),
        avoid_behaviors: splitTags(debugForm.avoidBehaviors),
        retrieval_query: debugForm.retrievalQuery,
        max_count: 20,
      })
      setDebugResult(result.data)
    } catch (error) {
      toast({
        title: '检索调试失败',
        description: error instanceof Error ? error.message : '无法完成行为检索调试',
        variant: 'destructive',
      })
    } finally {
      setDebugLoading(false)
    }
  }

  useEffect(() => {
    loadChats()
  }, [])

  useEffect(() => {
    loadPaths()
  }, [selectedSessionId, enabledFilter, page])

  useEffect(() => {
    if (selectedPathId !== null) {
      loadDetail(selectedPathId)
    }
  }, [selectedPathId])

  const applySearch = () => {
    setPage(1)
    loadPaths()
  }
  const toggleSceneGroup = (groupKey: string) => {
    setOpenSceneGroups((current) => {
      const next = new Set(current)
      if (next.has(groupKey)) {
        next.delete(groupKey)
      } else {
        next.add(groupKey)
      }
      return next
    })
  }

  return (
    <div className="mx-auto flex h-full w-full max-w-7xl flex-col gap-4 p-4 sm:p-6">
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <BrainCircuit className="h-5 w-5 text-primary" />
            <h1 className="text-2xl font-semibold tracking-normal">行为学习</h1>
          </div>
          <p className="mt-1 text-sm text-muted-foreground">
            浏览场景、行为、结果之间的经验路径和检索命中情况
          </p>
        </div>
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <Select
            value={selectedSessionId}
            onValueChange={(value) => {
              setSelectedSessionId(value)
              setPage(1)
            }}
          >
            <SelectTrigger className="w-full sm:w-64">
              <SelectValue placeholder="选择聊天流" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">全部聊天流</SelectItem>
              {chats.map((chat) => (
                <SelectItem key={chat.session_id || '__global__'} value={chat.session_id || '__global__'}>
                  {chat.display_name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button variant="outline" onClick={() => { loadChats(); loadPaths() }}>
            <RefreshCw className="mr-2 h-4 w-4" />
            刷新
          </Button>
        </div>
      </div>

      <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as ActiveTab)} className="min-h-0 flex-1">
        <TabsList className="grid w-full max-w-xl grid-cols-3">
          <TabsTrigger value="paths">经验路径</TabsTrigger>
          <TabsTrigger value="debug">检索调试</TabsTrigger>
          <TabsTrigger value="graph">局部图谱</TabsTrigger>
        </TabsList>

        <TabsContent value="paths" className="mt-4 min-h-0 space-y-4">
          <div className="flex flex-col gap-2 rounded-lg border bg-background p-3 sm:flex-row sm:items-center">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                onKeyDown={(event) => { if (event.key === 'Enter') applySearch() }}
                placeholder="搜索场景、行为、结果"
                className="pl-9"
              />
            </div>
            <Select value={enabledFilter} onValueChange={(value) => { setEnabledFilter(value); setPage(1) }}>
              <SelectTrigger className="w-full sm:w-36">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部状态</SelectItem>
                <SelectItem value="true">启用中</SelectItem>
                <SelectItem value="false">已停用</SelectItem>
              </SelectContent>
            </Select>
            <Button onClick={applySearch}>搜索</Button>
          </div>

          <div className="overflow-hidden rounded-lg border bg-background">
            <div className="flex items-center justify-between border-b px-4 py-3 text-sm text-muted-foreground">
              <span>{selectedChatName} · {total} 条经验路径</span>
              {loading && <Loader2 className="h-4 w-4 animate-spin" />}
            </div>
            <ScrollArea className="h-[560px]">
              <div className="divide-y">
                {paths.length === 0 && !loading ? (
                  <div className="p-8 text-center text-sm text-muted-foreground">暂无行为经验路径</div>
                ) : (
                  sceneGroups.map((group) => (
                    <SceneGroupRow
                      key={group.key}
                      group={group}
                      open={openSceneGroups.has(group.key)}
                      selectedPathId={selectedPathId}
                      onToggle={() => toggleSceneGroup(group.key)}
                      onSelectPath={(pathId) => {
                        setSelectedPathId(pathId)
                        setActiveTab('graph')
                      }}
                    />
                  ))
                )}
              </div>
            </ScrollArea>
            <div className="flex items-center justify-between border-t px-4 py-3">
              <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>
                <ChevronLeft className="mr-1 h-4 w-4" />
                上一页
              </Button>
              <span className="text-sm text-muted-foreground">{page} / {totalPages}</span>
              <Button
                variant="outline"
                size="sm"
                disabled={page >= totalPages}
                onClick={() => setPage((value) => value + 1)}
              >
                下一页
                <ChevronRight className="ml-1 h-4 w-4" />
              </Button>
            </div>
          </div>
        </TabsContent>

        <TabsContent value="debug" className="mt-4 grid gap-4 xl:grid-cols-[420px_minmax(0,1fr)]">
          <div className="space-y-3 rounded-lg border bg-background p-4">
            <h2 className="text-base font-semibold">输入场景画像</h2>
            <Field label="场景摘要">
              <Textarea value={debugForm.summary} onChange={(event) => setDebugForm({ ...debugForm, summary: event.target.value })} />
            </Field>
            <Field label="用户意图">
              <Input value={debugForm.userIntent} onChange={(event) => setDebugForm({ ...debugForm, userIntent: event.target.value })} />
            </Field>
            <Field label="对话阶段">
              <Input value={debugForm.conversationPhase} onChange={(event) => setDebugForm({ ...debugForm, conversationPhase: event.target.value })} />
            </Field>
            <Field label="领域标签">
              <Input value={debugForm.domainTags} onChange={(event) => setDebugForm({ ...debugForm, domainTags: event.target.value })} placeholder="用逗号分隔" />
            </Field>
            <Field label="行为需求">
              <Input value={debugForm.behaviorNeeds} onChange={(event) => setDebugForm({ ...debugForm, behaviorNeeds: event.target.value })} placeholder="用逗号分隔" />
            </Field>
            <Field label="检索查询">
              <Textarea value={debugForm.retrievalQuery} onChange={(event) => setDebugForm({ ...debugForm, retrievalQuery: event.target.value })} />
            </Field>
            <Button className="w-full" onClick={runDebug} disabled={debugLoading}>
              {debugLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <GitBranch className="mr-2 h-4 w-4" />}
              试跑检索
            </Button>
          </div>
          <RetrievalDebugView result={debugResult} />
        </TabsContent>

        <TabsContent value="graph" className="mt-4">
          <PathGraphView detail={detail} loading={detailLoading} />
        </TabsContent>
      </Tabs>
    </div>
  )
}

function SceneGroupRow({
  group,
  open,
  selectedPathId,
  onToggle,
  onSelectPath,
}: {
  group: BehaviorSceneGroup
  open: boolean
  selectedPathId: number | null
  onToggle: () => void
  onSelectPath: (pathId: number) => void
}) {
  const scene = parseSceneStart(group.trigger)
  return (
    <Collapsible open={open} onOpenChange={onToggle}>
      <div className="px-4 py-3">
        <CollapsibleTrigger asChild>
          <button
            type="button"
            className="flex w-full flex-col gap-3 rounded-lg p-2 text-left transition hover:bg-muted/60 lg:flex-row lg:items-start lg:justify-between"
          >
            <div className="min-w-0 flex-1 space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                {open ? (
                  <ChevronDown className="h-4 w-4 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-4 w-4 text-muted-foreground" />
                )}
                <Badge variant="outline">{group.paths.length} 个行为分支</Badge>
                <span className="text-xs text-muted-foreground">{group.chatName}</span>
                <span className="text-xs text-muted-foreground">更新 {formatTime(group.latestUpdate)}</span>
              </div>
              <p className="text-sm leading-6">
                <span className="text-muted-foreground">场景摘要：</span>
                {shortText(scene.summary, 130)}
              </p>
            </div>
            <div className="grid min-w-[220px] grid-cols-4 gap-2 text-center text-xs">
              <Metric label="最高分" value={formatScore(group.bestScore)} />
              <Metric label="使用" value={String(group.activationCount)} />
              <Metric label="成功" value={String(group.successCount)} />
              <Metric label="失败" value={String(group.failureCount)} />
            </div>
          </button>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <div className="mt-2 space-y-2 border-l pl-4">
            <SceneSummaryPanel sceneText={group.trigger} compact />
            {group.paths.map((path) => (
              <button
                key={path.id}
                type="button"
                onClick={() => onSelectPath(path.id)}
                className={cn(
                  'block w-full rounded-lg border bg-background px-3 py-3 text-left transition hover:bg-muted/60',
                  selectedPathId === path.id && 'border-primary bg-muted'
                )}
              >
                <div className="flex flex-col gap-2 lg:flex-row lg:items-start lg:justify-between">
                  <div className="min-w-0 flex-1 space-y-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant={path.enabled ? 'default' : 'secondary'}>{path.enabled ? '启用' : '停用'}</Badge>
                      <span className="text-xs text-muted-foreground">经验路径 #{path.id}</span>
                      <span className="text-xs text-muted-foreground">更新 {formatTime(path.update_time)}</span>
                    </div>
                    <p className="text-sm"><span className="text-muted-foreground">行为：</span>{shortText(path.action, 110)}</p>
                    <p className="text-sm"><span className="text-muted-foreground">结果：</span>{shortText(path.outcome, 110)}</p>
                  </div>
                  <div className="grid min-w-[220px] grid-cols-4 gap-2 text-center text-xs">
                    <Metric label="分数" value={formatScore(path.score)} />
                    <Metric label="使用" value={String(path.activation_count)} />
                    <Metric label="成功" value={String(path.success_count)} />
                    <Metric label="失败" value={String(path.failure_count)} />
                  </div>
                </div>
              </button>
            ))}
          </div>
        </CollapsibleContent>
      </div>
    </Collapsible>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border bg-muted/30 px-2 py-1">
      <div className="font-medium text-foreground">{value}</div>
      <div className="text-muted-foreground">{label}</div>
    </div>
  )
}

function SceneSummaryPanel({ sceneText, compact = false }: { sceneText: string; compact?: boolean }) {
  const scene = parseSceneStart(sceneText)
  const rows = [
    ['摘要', scene.summary],
    ['意图', scene.intent],
    ['阶段', scene.phase],
    ['标签', scene.domains],
    ['需求', scene.needs],
  ].filter(([, value]) => value)

  return (
    <div className={cn('rounded-lg border bg-muted/20', compact ? 'p-3' : 'p-4')}>
      <div className="mb-2 text-xs font-medium text-muted-foreground">场景</div>
      <div className="space-y-1.5">
        {rows.map(([label, value]) => (
          <div key={label} className={cn('grid gap-2 text-sm', compact ? 'md:grid-cols-[3rem_minmax(0,1fr)]' : 'md:grid-cols-[4rem_minmax(0,1fr)]')}>
            <span className="text-muted-foreground">{label}</span>
            <span className="min-w-0 break-words leading-6">{value}</span>
          </div>
        ))}
      </div>
      {rows.length <= 1 && scene.raw !== scene.summary && (
        <p className="mt-2 break-words text-xs leading-5 text-muted-foreground">{scene.raw}</p>
      )}
    </div>
  )
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="space-y-1.5">
      <Label className="text-xs text-muted-foreground">{label}</Label>
      {children}
    </div>
  )
}

function RetrievalDebugView({ result }: { result: BehaviorRetrievalDebugPayload | null }) {
  if (!result) {
    return (
      <div className="rounded-lg border bg-background p-8 text-center text-sm text-muted-foreground">
        输入场景画像后，可以看到描述节点、命中的场景节点和候选经验路径
      </div>
    )
  }
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Panel title="描述节点">
        <TokenList items={result.descriptors.map((item) => `${item.node_kind} · ${item.name} · ${item.weight}`)} />
      </Panel>
      <Panel title="命中节点">
        <NodeScoreList nodes={result.matched_nodes} />
      </Panel>
      <Panel title="扩展节点">
        <NodeScoreList nodes={result.expanded_nodes.slice(0, 20)} />
      </Panel>
      <Panel title="候选路径">
        <div className="space-y-3">
          {result.candidates.length === 0 ? (
            <p className="text-sm text-muted-foreground">没有命中候选</p>
          ) : (
            result.candidates.map((candidate) => (
              <div key={candidate.behavior_id} className="rounded-md border p-3 text-sm">
                <div className="mb-1 flex items-center justify-between">
                  <span className="font-medium">#{candidate.behavior_id}</span>
                  <Badge variant="outline">{formatScore(candidate.score)}</Badge>
                </div>
                {candidate.path ? (
                  <div className="space-y-1 text-muted-foreground">
                    <p>场景：{shortText(candidate.path.trigger, 56)}</p>
                    <p>行为：{shortText(candidate.path.action, 56)}</p>
                  </div>
                ) : (
                  <p className="text-muted-foreground">路径已不存在</p>
                )}
              </div>
            ))
          )}
        </div>
      </Panel>
    </div>
  )
}

function getBehaviorGraphNodeId(kind: string, id: number): string {
  if (kind === 'action') return `action:${id}`
  if (kind === 'outcome') return `outcome:${id}`
  if (kind === 'path') return `path:${id}`
  return `scene:${id}`
}

function hashBehaviorGraphText(value: string): number {
  return Array.from(value).reduce((hash, char) => (hash * 31 + char.charCodeAt(0)) % 997, 17)
}

function getSceneNodeColumn(kind: string): number {
  if (kind === 'scene') return 0
  if (kind === 'intent' || kind === 'phase') return 1
  if (kind === 'domain' || kind === 'need') return 2
  return 3
}

function getSceneNodeLane(kind: string): number {
  const laneByKind: Record<string, number> = {
    scene: 0,
    intent: -1,
    phase: 1,
    domain: -1,
    need: 1,
    risk: -1,
    avoid: 1,
  }
  return laneByKind[kind] ?? 0
}

function shouldShowBehaviorEdgeLabel(kind: string): boolean {
  return kind === 'scene_action' || kind === 'action_outcome'
}

function buildBehaviorFlowGraph(detail: BehaviorPathDetail): { nodes: BehaviorFlowNode[]; edges: BehaviorFlowEdge[] } {
  const sceneNodes = detail.nodes.filter((node) => node.kind !== 'action' && node.kind !== 'outcome')
  const actionNodes = detail.nodes.filter((node) => node.kind === 'action')
  const outcomeNodes = detail.nodes.filter((node) => node.kind === 'outcome')
  const layeredNodes = [
    ...sceneNodes,
    {
      id: detail.path.id,
      kind: 'path',
      label: `经验路径 #${detail.path.id}`,
      score: detail.path.score,
      source_count: detail.path.count,
    },
    ...actionNodes,
    ...outcomeNodes,
  ]
  const sceneColumnCounts = new Map<number, number>()
  sceneNodes.forEach((node) => {
    const column = getSceneNodeColumn(node.kind)
    sceneColumnCounts.set(column, (sceneColumnCounts.get(column) ?? 0) + 1)
  })
  const sceneColumnIndexes = new Map<number, number>()
  const actionOutcomeIndexes = new Map<string, number>()

  const nodes: BehaviorFlowNode[] = layeredNodes.map((node) => {
    let x = 0
    let y = 0

    if (node.kind === 'path') {
      x = 720
      y = -28
    } else if (node.kind === 'action' || node.kind === 'outcome') {
      const index = actionOutcomeIndexes.get(node.kind) ?? 0
      actionOutcomeIndexes.set(node.kind, index + 1)
      const count = node.kind === 'action' ? actionNodes.length : outcomeNodes.length
      const centeredIndex = index - (count - 1) / 2
      x = node.kind === 'action' ? 1030 : 1340
      y = centeredIndex * 150 + 18
    } else {
      const column = getSceneNodeColumn(node.kind)
      const index = sceneColumnIndexes.get(column) ?? 0
      const count = sceneColumnCounts.get(column) ?? 1
      sceneColumnIndexes.set(column, index + 1)
      const centeredIndex = index - (count - 1) / 2
      const hash = hashBehaviorGraphText(`${node.kind}:${node.id}:${node.label}`)
      const jitterX = (hash % 5) * 10
      const jitterY = ((hash % 7) - 3) * 8
      x = column * 190 + jitterX
      y = centeredIndex * 128 + getSceneNodeLane(node.kind) * 58 + jitterY
    }

    return {
      id: getBehaviorGraphNodeId(node.kind, node.id),
      type: 'behavior',
      position: { x, y },
      data: {
        kind: node.kind,
        label: shortText(node.label, node.kind === 'path' ? 36 : 72),
        detail: node.label,
      },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    }
  })

  const nodeIds = new Set(nodes.map((node) => node.id))
  const edges: BehaviorFlowEdge[] = detail.edges
    .filter((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target))
    .map((edge) => {
      const color = edge.kind === 'action_outcome'
        ? '#0284c7'
        : edge.kind === 'scene_action'
          ? '#059669'
          : edge.kind === 'co_occurs'
            ? '#94a3b8'
            : '#7c3aed'
      return {
        id: edge.id,
        source: edge.source,
        target: edge.target,
        type: edge.kind === 'co_occurs' ? 'straight' : 'bezier',
        animated: edge.kind === 'scene_action' || edge.kind === 'action_outcome',
        label: shouldShowBehaviorEdgeLabel(edge.kind) ? `${edge.kind} · ${formatScore(edge.weight)}` : undefined,
        interactionWidth: 18,
        markerEnd: {
          type: MarkerType.ArrowClosed,
          width: 14,
          height: 14,
          color,
        },
        style: {
          stroke: color,
          strokeWidth: Math.max(1.5, Math.min(4, edge.weight)),
          opacity: edge.kind === 'co_occurs' ? 0.25 : shouldShowBehaviorEdgeLabel(edge.kind) ? 0.82 : 0.48,
        },
        labelStyle: {
          fill: '#334155',
          fontSize: 11,
          fontWeight: 600,
        },
        labelBgPadding: [6, 2],
        labelBgBorderRadius: 6,
        labelBgStyle: { fill: 'rgba(255,255,255,0.92)', fillOpacity: 0.95 },
      }
    })

  return { nodes, edges }
}

function BehaviorFlowGraph({ detail }: { detail: BehaviorPathDetail }) {
  const { nodes, edges } = useMemo(() => buildBehaviorFlowGraph(detail), [detail])
  if (nodes.length === 0) {
    return <div className="rounded-lg border p-6 text-center text-sm text-muted-foreground">暂无可视化节点</div>
  }
  return (
    <div className="h-[640px] overflow-hidden rounded-lg border bg-background">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={behaviorNodeTypes}
        fitView
        fitViewOptions={{ padding: 0.18 }}
        minZoom={0.25}
        maxZoom={1.4}
        nodesDraggable
        nodesConnectable={false}
        elementsSelectable
        attributionPosition="bottom-left"
      >
        <Background variant={BackgroundVariant.Dots} gap={18} size={1} />
        <Controls />
      </ReactFlow>
    </div>
  )
}

function PathGraphView({ detail, loading }: { detail: BehaviorPathDetail | null; loading: boolean }) {
  if (loading) {
    return (
      <div className="rounded-lg border bg-background p-8 text-center text-sm text-muted-foreground">
        <Loader2 className="mx-auto mb-2 h-5 w-5 animate-spin" />
        正在读取局部图谱
      </div>
    )
  }
  if (!detail) {
    return <div className="rounded-lg border bg-background p-8 text-center text-sm text-muted-foreground">先选择一条经验路径</div>
  }
  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
      <div className="space-y-4 rounded-lg border bg-background p-4">
        <div>
          <h2 className="text-base font-semibold">#{detail.path.id} {detail.path.chat_name}</h2>
          <p className="mt-1 text-sm text-muted-foreground">最近更新 {formatTime(detail.path.update_time)}</p>
        </div>
        <SceneSummaryPanel sceneText={detail.path.trigger} />
        <div className="grid gap-3 md:grid-cols-2">
          <PathBlock title="行为" content={detail.path.action} />
          <PathBlock title="结果" content={detail.path.outcome} />
        </div>
        <Panel title="节点图">
          <BehaviorFlowGraph detail={detail} />
        </Panel>
        <Panel title="节点">
          <div className="grid gap-2 md:grid-cols-2">
            {detail.nodes.map((node, index) => (
              <div key={`${node.kind}-${node.id}-${index}`} className="rounded-md border p-3">
                <div className="mb-1 flex items-center gap-2">
                  <Badge variant="outline">{node.kind}</Badge>
                  <span className="text-xs text-muted-foreground">#{node.id}</span>
                </div>
                <p className="text-sm">{node.label}</p>
              </div>
            ))}
          </div>
        </Panel>
        <Panel title="边">
          <div className="space-y-2">
            {detail.edges.map((edge) => (
              <div key={edge.id} className="rounded-md border px-3 py-2 text-sm">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="secondary">{edge.kind}</Badge>
                  <span className="text-muted-foreground">{edge.source} → {edge.target}</span>
                  <span className="ml-auto text-xs">权重 {formatScore(edge.weight)} · {edge.count} 次</span>
                </div>
              </div>
            ))}
          </div>
        </Panel>
      </div>
      <div className="space-y-4">
        <Panel title="证据">
          <JsonList items={detail.evidence} />
        </Panel>
        <Panel title="反馈">
          <JsonList items={detail.feedback} />
        </Panel>
      </div>
    </div>
  )
}

function PathBlock({ title, content }: { title: string; content: string }) {
  return (
    <div className="rounded-lg border bg-muted/20 p-3">
      <div className="mb-2 text-xs font-medium text-muted-foreground">{title}</div>
      <p className="text-sm leading-6">{content || '-'}</p>
    </div>
  )
}

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="rounded-lg border bg-background p-4">
      <h2 className="mb-3 text-sm font-semibold">{title}</h2>
      {children}
    </section>
  )
}

function TokenList({ items }: { items: string[] }) {
  if (items.length === 0) return <p className="text-sm text-muted-foreground">暂无数据</p>
  return (
    <div className="flex flex-wrap gap-2">
      {items.map((item) => (
        <Badge key={item} variant="outline" className="max-w-full whitespace-normal break-all">
          {item}
        </Badge>
      ))}
    </div>
  )
}

function NodeScoreList({ nodes }: { nodes: Array<{ id: number | null; node_kind: string; name: string; match_score: number }> }) {
  if (nodes.length === 0) return <p className="text-sm text-muted-foreground">暂无数据</p>
  return (
    <div className="space-y-2">
      {nodes.map((node, index) => (
        <div key={`${node.id}-${index}`} className="rounded-md border px-3 py-2 text-sm">
          <div className="mb-1 flex items-center gap-2">
            <Badge variant="outline">{node.node_kind || 'node'}</Badge>
            <span className="text-xs text-muted-foreground">匹配 {formatScore(node.match_score)}</span>
          </div>
          <p>{node.name || '-'}</p>
        </div>
      ))}
    </div>
  )
}

function JsonList({ items }: { items: unknown[] }) {
  if (items.length === 0) return <p className="text-sm text-muted-foreground">暂无记录</p>
  return (
    <div className="space-y-2">
      {items.slice().reverse().map((item, index) => (
        <pre key={index} className="overflow-auto rounded-md border bg-muted/30 p-3 text-xs leading-5">
          {JSON.stringify(item, null, 2)}
        </pre>
      ))}
    </div>
  )
}
