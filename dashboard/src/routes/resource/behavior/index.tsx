import { memo, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import {
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  GitBranch,
  Loader2,
  RefreshCw,
  Search,
  X,
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
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
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
  listBehaviorClusters,
  listBehaviorChats,
  listBehaviorPaths,
  type BehaviorChatInfo,
  type BehaviorClusterItem,
  type BehaviorClusterTag,
  type BehaviorPathDetail,
  type BehaviorPathItem,
  type BehaviorRetrievalDebugPayload,
} from '@/lib/behavior-api'
import { cn } from '@/lib/utils'

const PAGE_SIZE = 20
const DEFAULT_CLUSTER_PAGE_SIZE = 200
const CLUSTER_PAGE_SIZE_OPTIONS = [200, 1000, 3000, 5000]

type ActiveTab = 'paths' | 'clusters' | 'debug' | 'graph'

interface BehaviorSceneGroup {
  key: string
  trigger: string
  sceneClusterId: number | null
  clusterName: string
  clusterTags: BehaviorClusterTag[]
  clusterSourceCount: number
  clusterScore: number
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

interface ClusterMapNodeData {
  label: string
  kind: 'cluster' | 'callout'
  detail: string
  tags: BehaviorClusterTag[]
  metric: string
  groupIndex: number
  groupName: string
  colorClassName: string
}

type BehaviorFlowNode = Node<BehaviorFlowNodeData>
type BehaviorFlowEdge = Edge
type ClusterMapNode = Node<ClusterMapNodeData>
type ClusterMapEdge = Edge

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

const ClusterMapNodeView = memo(({ data }: NodeProps<ClusterMapNodeData>) => {
  if (data.kind === 'callout') {
    const primaryTag = topClusterTags(data.tags, 1)[0]
    return (
      <div className="pointer-events-none w-64 rounded-lg border bg-background/96 p-3 text-left text-xs shadow-xl backdrop-blur-md">
        <Handle className="opacity-0" type="target" position={Position.Left} />
        <div className="mb-2 flex flex-wrap items-center gap-1.5">
          <Badge variant="secondary">{data.label}</Badge>
          <Badge variant="outline">{data.groupName}</Badge>
          <span className="text-muted-foreground">{data.metric}</span>
        </div>
        {primaryTag && (
          <div className="mb-2 rounded-md border bg-muted/30 px-2 py-1.5">
            <div className="break-words font-medium text-foreground">{primaryTag.tag}</div>
            <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-muted">
              <div className="h-full rounded-full bg-primary" style={{ width: formatProbability(primaryTag.probability) }} />
            </div>
          </div>
        )}
        <div className="line-clamp-3 leading-5 text-muted-foreground">{data.detail}</div>
      </div>
    )
  }

  return (
    <div className={cn(
      'group flex h-12 w-12 items-center justify-center rounded-full border text-center text-[10px] font-semibold shadow-[0_4px_10px_rgba(15,23,42,0.16)] transition hover:z-10 hover:shadow-xl hover:ring-2 hover:ring-primary/35',
      data.colorClassName
    )}
    title={`${data.detail}\n${data.groupName} · ${data.metric}`}>
      <Handle className="opacity-0" type="target" position={Position.Left} />
      <span className="opacity-0 transition group-hover:opacity-100">{data.label}</span>
      <Handle className="opacity-0" type="source" position={Position.Right} />
    </div>
  )
})

ClusterMapNodeView.displayName = 'ClusterMapNodeView'

const behaviorNodeTypes: NodeTypes = {
  behavior: BehaviorGraphNode,
  clusterMap: ClusterMapNodeView,
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

function formatProbability(value: number): string {
  if (!Number.isFinite(value)) return '0%'
  return `${Math.round(value * 100)}%`
}

function behaviorPathTypeLabel(path: Pick<BehaviorPathItem, 'actor_type' | 'learning_type'>): string {
  if (path.actor_type === 'maibot_self' && path.learning_type === 'self_reflection') return '自身反馈'
  if (path.actor_type === 'group_collective') return '群体观察'
  if (path.actor_type === 'other_user') return '他人观察'
  return '未知来源'
}

function isSelfReflectionPath(path: Pick<BehaviorPathItem, 'actor_type' | 'learning_type'>): boolean {
  return path.actor_type === 'maibot_self' && path.learning_type === 'self_reflection'
}

function topClusterTags(tags: BehaviorClusterTag[], maxCount = 6): BehaviorClusterTag[] {
  return tags
    .slice()
    .sort((left, right) => right.probability - left.probability)
    .slice(0, maxCount)
}

function clusterTitle(name: string, tags: BehaviorClusterTag[]): string {
  const tagNames = topClusterTags(tags, 3).map((item) => item.tag)
  if (tagNames.length > 0) return tagNames.join(' · ')
  return name || '未命名场景簇'
}

function latestTime(left: string | null, right: string | null): string | null {
  if (!left) return right
  if (!right) return left
  return right > left ? right : left
}

function buildClusterItemsFromPaths(paths: BehaviorPathItem[]): BehaviorClusterItem[] {
  const clusters = new Map<string, BehaviorClusterItem>()
  paths.forEach((path) => {
    const clusterKey = String(path.scene_cluster_id ?? (path.scene_cluster_name || path.trigger || path.id))
    const existing = clusters.get(clusterKey)
    if (!existing) {
      clusters.set(clusterKey, {
        id: path.scene_cluster_id,
        name: path.scene_cluster_name || path.trigger,
        tags: path.scene_cluster_tags,
        source_count: path.scene_cluster_source_count,
        score: path.scene_cluster_score,
        update_time: path.update_time,
        session_id: path.session_id,
        chat_name: path.chat_name,
        path_count: 1,
        enabled_path_count: path.enabled ? 1 : 0,
        activation_count: path.activation_count,
        success_count: path.success_count,
        failure_count: path.failure_count,
        observed_path_count: path.learning_type === 'observed_behavior' ? 1 : 0,
        self_reflection_path_count: path.learning_type === 'self_reflection' ? 1 : 0,
        last_active_time: path.last_active_time,
      })
      return
    }
    existing.path_count += 1
    existing.enabled_path_count += path.enabled ? 1 : 0
    existing.activation_count += path.activation_count
    existing.success_count += path.success_count
    existing.failure_count += path.failure_count
    existing.observed_path_count += path.learning_type === 'observed_behavior' ? 1 : 0
    existing.self_reflection_path_count += path.learning_type === 'self_reflection' ? 1 : 0
    existing.source_count = Math.max(existing.source_count, path.scene_cluster_source_count)
    existing.score = Math.max(existing.score, path.scene_cluster_score)
    existing.update_time = latestTime(existing.update_time, path.update_time)
    existing.last_active_time = latestTime(existing.last_active_time, path.last_active_time)
  })
  return Array.from(clusters.values()).sort((left, right) => (right.update_time ?? '').localeCompare(left.update_time ?? ''))
}

function stableHash(value: string): number {
  return Array.from(value).reduce((hash, char) => (hash * 33 + char.charCodeAt(0)) % 1000003, 5381)
}

const JS_DISTANCE_MAX = Math.sqrt(Math.log(2))
const CLUSTER_COLOR_CLASSES = [
  'border-[hsl(var(--primary))] bg-[hsl(var(--primary))] text-primary-foreground',
  'border-[hsl(var(--chart-1))] bg-[hsl(var(--chart-1))] text-white',
  'border-[hsl(var(--chart-2))] bg-[hsl(var(--chart-2))] text-white',
  'border-[hsl(var(--chart-3))] bg-[hsl(var(--chart-3))] text-white',
  'border-[hsl(var(--chart-4))] bg-[hsl(var(--chart-4))] text-white',
  'border-[hsl(var(--chart-5))] bg-[hsl(var(--chart-5))] text-white',
]

interface ClusterProjectionPoint {
  x: number
  y: number
}

interface ClusterMapGroup {
  index: number
  name: string
  colorClassName: string
  count: number
  topTags: BehaviorClusterTag[]
}

function clampNumber(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

function normalizeTagDistribution(tags: BehaviorClusterTag[]): Map<string, number> {
  const total = tags.reduce((sum, item) => sum + Math.max(item.probability, 0), 0)
  if (total <= 0) return new Map()
  return new Map(tags.map((item) => [item.tag, Math.max(item.probability, 0) / total]))
}

function jensenShannonDistance(
  leftTags: BehaviorClusterTag[],
  rightTags: BehaviorClusterTag[],
  tagUniverse: string[]
): number {
  const left = normalizeTagDistribution(leftTags)
  const right = normalizeTagDistribution(rightTags)
  let leftDivergence = 0
  let rightDivergence = 0
  tagUniverse.forEach((tag) => {
    const leftValue = left.get(tag) ?? 0
    const rightValue = right.get(tag) ?? 0
    const middle = (leftValue + rightValue) / 2
    if (leftValue > 0 && middle > 0) leftDivergence += leftValue * Math.log(leftValue / middle)
    if (rightValue > 0 && middle > 0) rightDivergence += rightValue * Math.log(rightValue / middle)
  })
  return Math.sqrt(Math.max(0, (leftDivergence + rightDivergence) / 2))
}

function similarityFromDistance(distance: number): number {
  return 1 - clampNumber(distance / JS_DISTANCE_MAX, 0, 1)
}

function buildDistanceMatrix(clusters: BehaviorClusterItem[], tagUniverse: string[]): number[][] {
  const distances = clusters.map(() => clusters.map(() => 0))
  for (let leftIndex = 0; leftIndex < clusters.length; leftIndex += 1) {
    for (let rightIndex = leftIndex + 1; rightIndex < clusters.length; rightIndex += 1) {
      const distance = jensenShannonDistance(clusters[leftIndex].tags, clusters[rightIndex].tags, tagUniverse)
      distances[leftIndex][rightIndex] = distance
      distances[rightIndex][leftIndex] = distance
    }
  }
  return distances
}

function normalizeVector(vector: number[]): number[] {
  const norm = Math.hypot(...vector)
  if (norm <= 1e-9) return vector.map(() => 0)
  return vector.map((value) => value / norm)
}

function multiplyMatrixVector(matrix: number[][], vector: number[]): number[] {
  return matrix.map((row) => row.reduce((sum, value, index) => sum + value * vector[index], 0))
}

function dotProduct(left: number[], right: number[]): number {
  return left.reduce((sum, value, index) => sum + value * right[index], 0)
}

function powerEigen(matrix: number[][], seed: number): { value: number; vector: number[] } {
  const size = matrix.length
  let vector = normalizeVector(Array.from({ length: size }, (_, index) => {
    const base = (index + 1) * (seed + 2)
    return Math.sin(base) + Math.cos(base * 0.7)
  }))
  for (let iteration = 0; iteration < 80; iteration += 1) {
    const next = normalizeVector(multiplyMatrixVector(matrix, vector))
    if (Math.hypot(...next) <= 1e-9) break
    vector = next
  }
  const multiplied = multiplyMatrixVector(matrix, vector)
  return { value: dotProduct(vector, multiplied), vector }
}

function classicalMdsProjection(distances: number[][]): ClusterProjectionPoint[] {
  const size = distances.length
  if (size === 0) return []
  if (size === 1) return [{ x: 0, y: 0 }]

  const squared = distances.map((row) => row.map((value) => value * value))
  const rowMeans = squared.map((row) => row.reduce((sum, value) => sum + value, 0) / size)
  const totalMean = rowMeans.reduce((sum, value) => sum + value, 0) / size
  const centered = squared.map((row, rowIndex) => (
    row.map((value, columnIndex) => -0.5 * (value - rowMeans[rowIndex] - rowMeans[columnIndex] + totalMean))
  ))

  const first = powerEigen(centered, 1)
  const deflated = centered.map((row, rowIndex) => (
    row.map((value, columnIndex) => value - first.value * first.vector[rowIndex] * first.vector[columnIndex])
  ))
  const second = powerEigen(deflated, 7)
  const firstScale = Math.sqrt(Math.max(first.value, 0))
  const secondScale = Math.sqrt(Math.max(second.value, 0))

  if (firstScale <= 1e-6 && secondScale <= 1e-6) {
    return Array.from({ length: size }, (_, index) => {
      const angle = (index / size) * Math.PI * 2
      return { x: Math.cos(angle), y: Math.sin(angle) }
    })
  }
  return first.vector.map((value, index) => ({
    x: value * firstScale,
    y: second.vector[index] * secondScale,
  }))
}

function scaleProjection(points: ClusterProjectionPoint[], width: number, height: number): ClusterProjectionPoint[] {
  if (points.length === 0) return []
  const minX = Math.min(...points.map((point) => point.x))
  const maxX = Math.max(...points.map((point) => point.x))
  const minY = Math.min(...points.map((point) => point.y))
  const maxY = Math.max(...points.map((point) => point.y))
  const spanX = Math.max(maxX - minX, 1e-6)
  const spanY = Math.max(maxY - minY, 1e-6)
  const scale = Math.min((width - 60) / spanX, (height - 55) / spanY)
  return points.map((point, index) => {
    const jitter = stableHash(`projection:${index}`)
    return {
      x: width / 2 + (point.x - (minX + maxX) / 2) * scale + (jitter % 41) - 20,
      y: height / 2 + (point.y - (minY + maxY) / 2) * scale + ((jitter >> 4) % 41) - 20,
    }
  })
}

function chooseGroupCount(count: number): number {
  if (count <= 1) return count
  if (count < 4) return Math.min(count, 2)
  return clampNumber(Math.round(Math.sqrt(count / 1.8)), 2, 6)
}

function assignProjectionGroups(points: ClusterProjectionPoint[]): number[] {
  const groupCount = chooseGroupCount(points.length)
  if (groupCount <= 1) return points.map(() => 0)

  const centroids: ClusterProjectionPoint[] = [points[0]]
  while (centroids.length < groupCount) {
    let bestIndex = 0
    let bestDistance = -1
    points.forEach((point, index) => {
      const distance = Math.min(...centroids.map((centroid) => Math.hypot(point.x - centroid.x, point.y - centroid.y)))
      if (distance > bestDistance) {
        bestDistance = distance
        bestIndex = index
      }
    })
    centroids.push(points[bestIndex])
  }

  let assignments = points.map(() => 0)
  for (let iteration = 0; iteration < 24; iteration += 1) {
    assignments = points.map((point) => {
      let bestGroup = 0
      let bestDistance = Number.POSITIVE_INFINITY
      centroids.forEach((centroid, groupIndex) => {
        const distance = Math.hypot(point.x - centroid.x, point.y - centroid.y)
        if (distance < bestDistance) {
          bestDistance = distance
          bestGroup = groupIndex
        }
      })
      return bestGroup
    })

    centroids.forEach((_, groupIndex) => {
      const members = points.filter((_point, index) => assignments[index] === groupIndex)
      if (members.length === 0) return
      centroids[groupIndex] = {
        x: members.reduce((sum, point) => sum + point.x, 0) / members.length,
        y: members.reduce((sum, point) => sum + point.y, 0) / members.length,
      }
    })
  }
  return assignments
}

function relaxProjectedPositions(points: ClusterProjectionPoint[], width: number, height: number): ClusterProjectionPoint[] {
  const positions = points.map((point) => ({ ...point }))
  for (let iteration = 0; iteration < 140; iteration += 1) {
    const forces = positions.map((position, index) => ({
      x: (points[index].x - position.x) * 0.012,
      y: (points[index].y - position.y) * 0.012,
    }))
    for (let leftIndex = 0; leftIndex < positions.length; leftIndex += 1) {
      for (let rightIndex = leftIndex + 1; rightIndex < positions.length; rightIndex += 1) {
        const left = positions[leftIndex]
        const right = positions[rightIndex]
        let dx = right.x - left.x
        let dy = right.y - left.y
        const rawDistance = Math.hypot(dx, dy)
        if (rawDistance < 0.001) {
          const angle = (stableHash(`relax:${leftIndex}:${rightIndex}`) % 360) * Math.PI / 180
          dx = Math.cos(angle)
          dy = Math.sin(angle)
        }
        const distance = Math.max(Math.hypot(dx, dy), 1)
        const minDistance = 84
        if (distance < minDistance) {
          const push = (minDistance - distance) * 0.09
          const pushX = (dx / distance) * push
          const pushY = (dy / distance) * push
          forces[leftIndex].x -= pushX
          forces[leftIndex].y -= pushY
          forces[rightIndex].x += pushX
          forces[rightIndex].y += pushY
        }
      }
    }
    positions.forEach((position, index) => {
      position.x = clampNumber(position.x + forces[index].x, 24, width - 38)
      position.y = clampNumber(position.y + forces[index].y, 24, height - 42)
    })
  }
  return positions
}

function summarizeClusterGroups(
  clusters: BehaviorClusterItem[],
  groupIndexes: number[]
): ClusterMapGroup[] {
  const groups = new Map<number, { count: number; tags: Map<string, number> }>()
  clusters.forEach((cluster, index) => {
    const groupIndex = groupIndexes[index] ?? 0
    const group = groups.get(groupIndex) ?? { count: 0, tags: new Map<string, number>() }
    group.count += 1
    cluster.tags.forEach((tag) => {
      group.tags.set(tag.tag, (group.tags.get(tag.tag) ?? 0) + tag.probability)
    })
    groups.set(groupIndex, group)
  })
  return Array.from(groups.entries())
    .sort(([leftIndex], [rightIndex]) => leftIndex - rightIndex)
    .map(([index, group]) => ({
      index,
      name: `区 ${index + 1}`,
      colorClassName: CLUSTER_COLOR_CLASSES[index % CLUSTER_COLOR_CLASSES.length],
      count: group.count,
      topTags: Array.from(group.tags.entries())
        .sort((left, right) => right[1] - left[1])
        .slice(0, 8)
        .map(([tag, probability]) => ({ tag, probability: probability / Math.max(group.count, 1) })),
    }))
}

function buildClusterMapGraph(clusters: BehaviorClusterItem[]): {
  nodes: ClusterMapNode[]
  edges: ClusterMapEdge[]
  clusterByNodeId: Map<string, BehaviorClusterItem>
  groups: ClusterMapGroup[]
} {
  const width = 1800
  const height = 980
  const tagUniverse = Array.from(new Set(clusters.flatMap((cluster) => cluster.tags.map((tag) => tag.tag)))).sort()
  const distances = buildDistanceMatrix(clusters, tagUniverse)
  const projected = scaleProjection(classicalMdsProjection(distances), width, height)
  const groupIndexes = assignProjectionGroups(projected)
  const clusterPositions = relaxProjectedPositions(projected, width, height)
  const groups = summarizeClusterGroups(clusters, groupIndexes)
  const groupByIndex = new Map(groups.map((group) => [group.index, group]))

  const clusterByNodeId = new Map<string, BehaviorClusterItem>()
  const nodes: ClusterMapNode[] = clusters.map((cluster, index) => {
    const clusterId = `cluster:${cluster.id ?? `${cluster.session_id}-${index}`}`
    const position = clusterPositions[index]
    const group = groupByIndex.get(groupIndexes[index] ?? 0) ?? groups[0]
    clusterByNodeId.set(clusterId, cluster)
    return {
      id: clusterId,
      type: 'clusterMap',
      position: {
        x: position.x - 12,
        y: position.y - 12,
      },
      data: {
        kind: 'cluster',
        label: `#${cluster.id ?? index + 1}`,
        detail: clusterTitle(cluster.name, cluster.tags),
        tags: cluster.tags,
        metric: `${cluster.path_count} 分支`,
        groupIndex: group?.index ?? 0,
        groupName: group?.name ?? '区 1',
        colorClassName: group?.colorClassName ?? CLUSTER_COLOR_CLASSES[0],
      },
    }
  })

  const similarityEdges: Array<{ left: number; right: number; similarity: number }> = []
  for (let leftIndex = 0; leftIndex < clusters.length; leftIndex += 1) {
    for (let rightIndex = leftIndex + 1; rightIndex < clusters.length; rightIndex += 1) {
      const similarity = similarityFromDistance(distances[leftIndex][rightIndex])
      if (similarity >= 0.62) {
        similarityEdges.push({ left: leftIndex, right: rightIndex, similarity })
      }
    }
  }

  const edges: ClusterMapEdge[] = similarityEdges
    .sort((left, right) => right.similarity - left.similarity)
    .slice(0, 18)
    .map((edge) => {
      const leftCluster = clusters[edge.left]
      const rightCluster = clusters[edge.right]
      const source = `cluster:${leftCluster.id ?? `${leftCluster.session_id}-${edge.left}`}`
      const target = `cluster:${rightCluster.id ?? `${rightCluster.session_id}-${edge.right}`}`
      const sameGroup = groupIndexes[edge.left] === groupIndexes[edge.right]
      return {
        id: `similarity:${source}:${target}`,
        source,
        target,
        type: 'bezier',
        animated: edge.similarity >= 0.72,
        style: {
          stroke: sameGroup ? '#0f766e' : '#64748b',
          strokeWidth: Math.max(1, edge.similarity * 2.2),
          opacity: sameGroup ? 0.18 : 0.1,
        },
      }
    })

  return { nodes, edges, clusterByNodeId, groups }
}

function buildClusterHoverGraph(
  hoveredNodeId: string | null,
  nodes: ClusterMapNode[],
  edges: ClusterMapEdge[],
  clusterByNodeId: Map<string, BehaviorClusterItem>
): { nodes: ClusterMapNode[]; edges: ClusterMapEdge[] } {
  if (!hoveredNodeId || !clusterByNodeId.has(hoveredNodeId)) return { nodes, edges }

  const sourceNode = nodes.find((node) => node.id === hoveredNodeId)
  const cluster = clusterByNodeId.get(hoveredNodeId)
  if (!sourceNode || !cluster) return { nodes, edges }

  const placeRight = sourceNode.position.x < 1320
  const calloutId = `callout:${hoveredNodeId}`
  const calloutNode: ClusterMapNode = {
    id: calloutId,
    type: 'clusterMap',
    position: {
      x: sourceNode.position.x + (placeRight ? 150 : -300),
      y: sourceNode.position.y - 64,
    },
    selectable: false,
    draggable: false,
    data: {
      ...sourceNode.data,
      kind: 'callout',
      label: sourceNode.data.label,
      detail: clusterTitle(cluster.name, cluster.tags),
      tags: cluster.tags,
      metric: `${cluster.path_count} 分支`,
    },
  }

  const calloutEdge: ClusterMapEdge = {
    id: `callout-edge:${hoveredNodeId}`,
    source: hoveredNodeId,
    target: calloutId,
    type: 'straight',
    animated: true,
    markerEnd: {
      type: MarkerType.ArrowClosed,
      color: 'hsl(var(--primary))',
    },
    style: {
      stroke: 'hsl(var(--primary))',
      strokeDasharray: '6 4',
      strokeWidth: 1.8,
      opacity: 0.82,
    },
  }

  return {
    nodes: [...nodes, calloutNode],
    edges: [...edges, calloutEdge],
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
  const [clusters, setClusters] = useState<BehaviorClusterItem[]>([])
  const [openSceneGroups, setOpenSceneGroups] = useState<Set<string>>(new Set())
  const [total, setTotal] = useState(0)
  const [clusterTotal, setClusterTotal] = useState(0)
  const [clusterPageSize, setClusterPageSize] = useState(DEFAULT_CLUSTER_PAGE_SIZE)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(false)
  const [clusterLoading, setClusterLoading] = useState(false)
  const [selectedPathId, setSelectedPathId] = useState<number | null>(null)
  const [detail, setDetail] = useState<BehaviorPathDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [debugLoading, setDebugLoading] = useState(false)
  const [debugResult, setDebugResult] = useState<BehaviorRetrievalDebugPayload | null>(null)
  const [debugForm, setDebugForm] = useState({
    summary: '',
    domainTags: '',
    behaviorNeeds: '',
    otherTraits: '',
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
      const clusterKey = path.scene_cluster_id ?? (path.scene_cluster_name || path.trigger)
      const key = `${path.session_id || '__global__'}::cluster:${clusterKey}`
      const existing = groups.get(key)
      if (!existing) {
        groups.set(key, {
          key,
          trigger: path.trigger,
          sceneClusterId: path.scene_cluster_id,
          clusterName: path.scene_cluster_name || path.trigger,
          clusterTags: path.scene_cluster_tags,
          clusterSourceCount: path.scene_cluster_source_count,
          clusterScore: path.scene_cluster_score,
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

  const loadPaths = async (targetPage = page) => {
    try {
      setLoading(true)
      const result = await listBehaviorPaths({
        session_id: selectedSessionId,
        search,
        enabled: enabledFilter,
        page: targetPage,
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

  const loadClusters = async () => {
    try {
      setClusterLoading(true)
      const result = await listBehaviorClusters({
        session_id: selectedSessionId,
        search,
        page: 1,
        page_size: clusterPageSize,
      })
      setClusters(result.data)
      setClusterTotal(result.total)
    } catch (error) {
      try {
        const fallbackResult = await listBehaviorPaths({
          session_id: selectedSessionId,
          search,
          enabled: 'all',
          page: 1,
          page_size: clusterPageSize,
        })
        const fallbackClusters = buildClusterItemsFromPaths(fallbackResult.data)
        setClusters(fallbackClusters)
        setClusterTotal(fallbackClusters.length)
      } catch {
        toast({
          title: '加载场景簇失败',
          description: error instanceof Error ? error.message : '无法读取行为场景簇',
          variant: 'destructive',
        })
      }
    } finally {
      setClusterLoading(false)
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
        retrieval_mode: 'tag_expand_scene_cluster',
        summary: debugForm.summary,
        tag_clusters: splitTags(debugForm.domainTags).map((tag) => ({ tag_name: tag, tag_aliases: [] })),
        need: { tag_name: splitTags(debugForm.behaviorNeeds)[0] ?? '', tag_aliases: [] },
        other_traits: splitTags(debugForm.otherTraits).map((tag) => ({ tag_name: tag, tag_aliases: [] })),
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
    loadClusters()
  }, [selectedSessionId, clusterPageSize])

  useEffect(() => {
    if (selectedPathId !== null) {
      loadDetail(selectedPathId)
    }
  }, [selectedPathId])

  const applySearch = () => {
    setPage(1)
    loadPaths(1)
    loadClusters()
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
          <h1 className="text-2xl font-semibold tracking-normal">行为学习</h1>
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
                  {chat.display_name} · {chat.cluster_count} 簇
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button variant="outline" onClick={() => { loadChats(); loadPaths(); loadClusters() }}>
            <RefreshCw className="mr-2 h-4 w-4" />
            刷新
          </Button>
        </div>
      </div>

      <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as ActiveTab)} className="min-h-0 flex-1">
        <TabsList className="grid w-full max-w-2xl grid-cols-4">
          <TabsTrigger value="paths">经验路径</TabsTrigger>
          <TabsTrigger value="clusters">场景簇浏览</TabsTrigger>
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
                placeholder="搜索场景簇 tag、行为、结果"
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
              <span>{selectedChatName} · {sceneGroups.length} 个场景簇 · {total} 条经验路径</span>
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

        <TabsContent value="clusters" className="mt-4 min-h-0 space-y-4">
          <div className="flex flex-col gap-2 rounded-lg border bg-background p-3 sm:flex-row sm:items-center">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                onKeyDown={(event) => { if (event.key === 'Enter') applySearch() }}
                placeholder="搜索场景簇 tag 或聊天流"
                className="pl-9"
              />
            </div>
            <Button onClick={applySearch}>搜索</Button>
            <Select value={String(clusterPageSize)} onValueChange={(value) => setClusterPageSize(Number(value))}>
              <SelectTrigger className="w-full sm:w-36">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {CLUSTER_PAGE_SIZE_OPTIONS.map((size) => (
                  <SelectItem key={size} value={String(size)}>
                    {size} 个
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <ClusterBrowserView
            clusters={clusters}
            total={clusterTotal}
            selectedChatName={selectedChatName}
            loading={clusterLoading}
          />
        </TabsContent>

        <TabsContent value="debug" className="mt-4 grid gap-4 xl:grid-cols-[420px_minmax(0,1fr)]">
          <div className="space-y-3 rounded-lg border bg-background p-4">
            <h2 className="text-base font-semibold">输入场景画像</h2>
            <Field label="场景摘要">
              <Textarea value={debugForm.summary} onChange={(event) => setDebugForm({ ...debugForm, summary: event.target.value })} />
            </Field>
            <Field label="领域标签">
              <Input value={debugForm.domainTags} onChange={(event) => setDebugForm({ ...debugForm, domainTags: event.target.value })} placeholder="用逗号分隔" />
            </Field>
            <Field label="行为需求">
              <Input value={debugForm.behaviorNeeds} onChange={(event) => setDebugForm({ ...debugForm, behaviorNeeds: event.target.value })} placeholder="用逗号分隔" />
            </Field>
            <Field label="他人特点/态度">
              <Input value={debugForm.otherTraits} onChange={(event) => setDebugForm({ ...debugForm, otherTraits: event.target.value })} placeholder="用逗号分隔" />
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

function ClusterBrowserView({
  clusters,
  total,
  selectedChatName,
  loading,
}: {
  clusters: BehaviorClusterItem[]
  total: number
  selectedChatName: string
  loading: boolean
}) {
  const { nodes, edges, clusterByNodeId, groups } = useMemo(() => buildClusterMapGraph(clusters), [clusters])
  const [selectedClusterNodeId, setSelectedClusterNodeId] = useState<string | null>(null)
  const [hoveredClusterNodeId, setHoveredClusterNodeId] = useState<string | null>(null)
  const [hoveredGroupIndex, setHoveredGroupIndex] = useState<number | null>(null)
  const selectedCluster = selectedClusterNodeId && clusterByNodeId.has(selectedClusterNodeId)
    ? clusterByNodeId.get(selectedClusterNodeId) ?? null
    : null
  const { nodes: displayNodes, edges: displayEdges } = useMemo(
    () => buildClusterHoverGraph(hoveredClusterNodeId, nodes, edges, clusterByNodeId),
    [clusterByNodeId, edges, hoveredClusterNodeId, nodes]
  )
  return (
    <div className="overflow-hidden rounded-lg border bg-background">
      <div className="flex flex-col gap-2 border-b px-4 py-3 text-sm text-muted-foreground lg:flex-row lg:items-center lg:justify-between">
        <div className="min-w-0">
          <div>{selectedChatName} · {total} 个场景簇</div>
          <div className="text-xs">距离越近表示 tag 概率分布越相似，颜色表示自动聚类分区。悬停点会引出简要说明，点击查看完整分布。</div>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          {groups.map((group) => (
            <ClusterGroupLegend
              key={group.index}
              group={group}
              open={hoveredGroupIndex === group.index}
              onHover={() => setHoveredGroupIndex(group.index)}
              onLeave={() => setHoveredGroupIndex(null)}
            />
          ))}
        </div>
        {loading && <Loader2 className="h-4 w-4 animate-spin" />}
      </div>
      {clusters.length === 0 && !loading ? (
        <div className="p-8 text-center text-sm text-muted-foreground">暂无场景簇</div>
      ) : (
        <div
          className="relative h-[620px] overflow-hidden bg-muted/10"
          onMouseLeave={() => setHoveredClusterNodeId(null)}
        >
          <ReactFlow
            nodes={displayNodes}
            edges={displayEdges}
            nodeTypes={behaviorNodeTypes}
            fitView
            fitViewOptions={{ padding: 0.12 }}
            minZoom={0.04}
            maxZoom={8}
            nodesDraggable
            nodesConnectable={false}
            elementsSelectable
            attributionPosition="bottom-left"
            onNodeClick={(_event, node) => {
              if (clusterByNodeId.has(node.id)) {
                setSelectedClusterNodeId(node.id)
              }
            }}
            onNodeMouseEnter={(_event, node) => {
              if (clusterByNodeId.has(node.id)) {
                setHoveredClusterNodeId((current) => current === node.id ? current : node.id)
              }
            }}
            onPaneClick={() => {
              setSelectedClusterNodeId(null)
              setHoveredClusterNodeId(null)
            }}
          >
            <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
            <Controls />
          </ReactFlow>
          <div
            data-behavior-cluster-drawer="true"
            data-open={selectedCluster ? "true" : "false"}
            className={cn(
              'absolute bottom-4 right-4 top-4 w-[min(420px,calc(100%-2rem))] transition duration-200 ease-out',
              selectedCluster ? 'pointer-events-auto translate-x-0 opacity-100' : 'pointer-events-none translate-x-[calc(100%+2rem)] opacity-0'
            )}
          >
            <div className="flex h-full flex-col overflow-hidden rounded-lg border bg-background/96 shadow-2xl backdrop-blur-md supports-[backdrop-filter]:bg-background/88">
              <div className="flex items-center justify-between border-b bg-background/95 px-4 py-3 backdrop-blur-md">
                <div>
                  <h2 className="text-sm font-semibold">场景簇详情</h2>
                  <p className="text-xs text-muted-foreground">tag 概率分布与关联行为统计</p>
                </div>
                <Button variant="ghost" size="icon" onClick={() => setSelectedClusterNodeId(null)} aria-label="关闭场景簇详情">
                  <X className="h-4 w-4" />
                </Button>
              </div>
              <ScrollArea className="min-h-0 flex-1 bg-background/90 p-4 backdrop-blur-md">
                {selectedCluster && <ClusterGraphDetail cluster={selectedCluster} />}
              </ScrollArea>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function ClusterGroupLegend({
  group,
  open,
  onHover,
  onLeave,
}: {
  group: ClusterMapGroup
  open: boolean
  onHover: () => void
  onLeave: () => void
}) {
  return (
    <Popover open={open}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className={cn(
            'rounded-md border px-2 py-0.5 text-[11px] leading-5 transition hover:shadow-sm',
            group.colorClassName
          )}
          onMouseEnter={onHover}
          onMouseLeave={onLeave}
        >
          {group.name} · {group.count}
        </button>
      </PopoverTrigger>
      <PopoverContent
        align="end"
        className="w-80"
        onMouseEnter={onHover}
        onMouseLeave={onLeave}
      >
        <div className="space-y-3">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="text-sm font-semibold">{group.name}</div>
              <div className="text-xs text-muted-foreground">{group.count} 个场景簇</div>
            </div>
          </div>
          <div className="space-y-2">
            {group.topTags.map((tag) => (
              <div key={tag.tag} className="grid gap-2 text-xs sm:grid-cols-[minmax(0,1fr)_3.5rem] sm:items-center">
                <div className="min-w-0">
                  <div className="mb-1 flex items-center justify-between gap-2">
                    <span className="break-words text-foreground">{tag.tag}</span>
                    <span className="shrink-0 text-muted-foreground sm:hidden">{formatProbability(tag.probability)}</span>
                  </div>
                  <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                    <div className="h-full rounded-full bg-primary" style={{ width: formatProbability(tag.probability) }} />
                  </div>
                </div>
                <span className="hidden text-right text-muted-foreground sm:block">{formatProbability(tag.probability)}</span>
              </div>
            ))}
          </div>
        </div>
      </PopoverContent>
    </Popover>
  )
}

function ClusterGraphDetail({ cluster }: { cluster: BehaviorClusterItem }) {
  const title = clusterTitle(cluster.name, cluster.tags)
  const activeRate = cluster.path_count > 0 ? cluster.enabled_path_count / cluster.path_count : 0
  const hasSelfReflection = (cluster.self_reflection_path_count ?? 0) > 0
  return (
    <div className="space-y-3">
      <div className="rounded-lg border bg-background/95 p-3 shadow-sm">
        <div className="min-w-0 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            {cluster.id !== null && <Badge variant="secondary">场景簇 #{cluster.id}</Badge>}
            <Badge variant="outline">{cluster.chat_name}</Badge>
          </div>
          <h2 className="break-words text-sm font-semibold leading-6">{title}</h2>
          <p className="text-xs text-muted-foreground">更新 {formatTime(cluster.update_time)} · 最近使用 {formatTime(cluster.last_active_time)}</p>
        </div>
      </div>
      <ClusterDistributionPanel
        name={cluster.name}
        tags={cluster.tags}
        sourceCount={cluster.source_count}
        score={cluster.score}
        compact
      />
      <div className="mt-3 grid gap-2 text-xs sm:grid-cols-5">
        <Metric label="分支" value={String(cluster.path_count)} />
        <Metric label="启用" value={String(cluster.enabled_path_count)} />
        <Metric label="观察" value={String(cluster.observed_path_count ?? 0)} />
        <Metric label="自身" value={String(cluster.self_reflection_path_count ?? 0)} />
        <Metric label="样本" value={String(cluster.source_count)} />
        <Metric label="簇分" value={formatScore(cluster.score)} />
        <Metric label="使用" value={String(cluster.activation_count)} />
        {hasSelfReflection && <Metric label="正向" value={String(cluster.success_count)} />}
        {hasSelfReflection && <Metric label="负向" value={String(cluster.failure_count)} />}
        <Metric label="启用率" value={formatProbability(activeRate)} />
      </div>
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
  const title = clusterTitle(group.clusterName, group.clusterTags)
  const selfReflectionPaths = group.paths.filter(isSelfReflectionPath)
  const selfSuccessCount = selfReflectionPaths.reduce((sum, path) => sum + path.success_count, 0)
  const selfFailureCount = selfReflectionPaths.reduce((sum, path) => sum + path.failure_count, 0)
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
                {group.sceneClusterId !== null && <Badge variant="secondary">场景簇 #{group.sceneClusterId}</Badge>}
                <span className="text-xs text-muted-foreground">{group.chatName}</span>
                <span className="text-xs text-muted-foreground">更新 {formatTime(group.latestUpdate)}</span>
              </div>
              <p className="text-sm leading-6">
                <span className="text-muted-foreground">触发分布：</span>
                {shortText(title, 130)}
              </p>
              <ClusterTagPills tags={group.clusterTags} maxCount={5} />
            </div>
            <div className={cn(
              'grid min-w-[220px] gap-2 text-center text-xs',
              selfReflectionPaths.length > 0 ? 'grid-cols-4' : 'grid-cols-2'
            )}>
              <Metric label="最高分" value={formatScore(group.bestScore)} />
              <Metric label="使用" value={String(group.activationCount)} />
              {selfReflectionPaths.length > 0 && <Metric label="正向" value={String(selfSuccessCount)} />}
              {selfReflectionPaths.length > 0 && <Metric label="负向" value={String(selfFailureCount)} />}
            </div>
          </button>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <div className="mt-2 space-y-2 border-l pl-4">
            <ClusterDistributionPanel
              name={group.clusterName}
              tags={group.clusterTags}
              sourceCount={group.clusterSourceCount}
              score={group.clusterScore}
              compact
            />
            {group.paths.map((path) => {
              const isSelfPath = isSelfReflectionPath(path)
              return (
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
                        <Badge variant={isSelfPath ? 'default' : 'outline'}>
                          {behaviorPathTypeLabel(path)}
                        </Badge>
                        <span className="text-xs text-muted-foreground">经验路径 #{path.id}</span>
                        <span className="text-xs text-muted-foreground">更新 {formatTime(path.update_time)}</span>
                      </div>
                      <p className="text-sm"><span className="text-muted-foreground">行为：</span>{shortText(path.action, 110)}</p>
                      <p className="text-sm"><span className="text-muted-foreground">结果：</span>{shortText(path.outcome, 110)}</p>
                    </div>
                    <div className={cn(
                      'grid min-w-[220px] gap-2 text-center text-xs',
                      isSelfPath ? 'grid-cols-5' : 'grid-cols-3'
                    )}>
                      <Metric label="分数" value={formatScore(path.score)} />
                      <Metric label="样本" value={String(path.count)} />
                      <Metric label="使用" value={String(path.activation_count)} />
                      {isSelfPath && <Metric label="正向" value={String(path.success_count)} />}
                      {isSelfPath && <Metric label="负向" value={String(path.failure_count)} />}
                    </div>
                  </div>
                </button>
              )
            })}
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

function ClusterTagPills({ tags, maxCount = 6 }: { tags: BehaviorClusterTag[]; maxCount?: number }) {
  const visibleTags = topClusterTags(tags, maxCount)
  if (visibleTags.length === 0) return null
  return (
    <div className="flex flex-wrap gap-1.5">
      {visibleTags.map((item) => (
        <Badge key={item.tag} variant="outline" className="max-w-full whitespace-normal break-all text-[11px]">
          {item.tag} · {formatProbability(item.probability)}
        </Badge>
      ))}
    </div>
  )
}

function ClusterDistributionPanel({
  name,
  tags,
  sourceCount,
  score,
  compact = false,
}: {
  name: string
  tags: BehaviorClusterTag[]
  sourceCount?: number
  score?: number
  compact?: boolean
}) {
  const visibleTags = topClusterTags(tags, compact ? 8 : 12)
  return (
    <div className={cn('rounded-lg border bg-muted/20', compact ? 'p-3' : 'p-4')}>
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <span className="text-xs font-medium text-muted-foreground">场景簇</span>
        {sourceCount !== undefined && <Badge variant="outline">样本 {sourceCount}</Badge>}
        {score !== undefined && <Badge variant="outline">簇分 {formatScore(score)}</Badge>}
      </div>
      <p className="mb-3 break-words text-sm font-medium leading-6">{clusterTitle(name, tags)}</p>
      {visibleTags.length === 0 ? (
        <p className="text-sm text-muted-foreground">{name || '暂无 tag 分布'}</p>
      ) : (
        <div className="space-y-2">
          {visibleTags.map((item) => (
            <div key={item.tag} className="grid gap-2 text-sm sm:grid-cols-[minmax(0,1fr)_4rem] sm:items-center">
              <div className="min-w-0">
                <div className="mb-1 flex items-center justify-between gap-2">
                  <span className="break-words text-xs text-foreground">{item.tag}</span>
                  <span className="shrink-0 text-xs text-muted-foreground">{formatProbability(item.probability)}</span>
                </div>
                <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                  <div className="h-full rounded-full bg-primary" style={{ width: formatProbability(item.probability) }} />
                </div>
              </div>
              <span className="hidden text-right text-xs text-muted-foreground sm:block">{item.probability.toFixed(3)}</span>
            </div>
          ))}
        </div>
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
        输入场景画像后，可以看到命中的场景簇、描述节点和候选经验路径
      </div>
    )
  }
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Panel title="描述节点">
        <TokenList items={result.descriptors.map((item) => `${item.node_kind} · ${item.name} · ${item.weight}`)} />
      </Panel>
      <Panel title="命中场景簇">
        <ClusterScoreList clusters={result.matched_clusters} />
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
                    <p>场景簇：{shortText(clusterTitle(candidate.path.scene_cluster_name, candidate.path.scene_cluster_tags), 56)}</p>
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
  const isSelfPath = isSelfReflectionPath(detail.path)
  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
      <div className="space-y-4 rounded-lg border bg-background p-4">
        <div>
          <h2 className="text-base font-semibold">#{detail.path.id} {detail.path.chat_name}</h2>
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <Badge variant={detail.path.learning_type === 'self_reflection' ? 'default' : 'outline'}>
              {behaviorPathTypeLabel(detail.path)}
            </Badge>
            <span className="text-sm text-muted-foreground">最近更新 {formatTime(detail.path.update_time)}</span>
          </div>
        </div>
        <ClusterDistributionPanel
          name={detail.scene_cluster.name || detail.path.scene_cluster_name || detail.path.trigger}
          tags={detail.scene_cluster.tags.length > 0 ? detail.scene_cluster.tags : detail.path.scene_cluster_tags}
          sourceCount={detail.scene_cluster.source_count || detail.path.scene_cluster_source_count}
          score={detail.scene_cluster.score || detail.path.scene_cluster_score}
        />
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
        {isSelfPath ? (
          <Panel title="反馈">
            <JsonList items={detail.feedback} />
          </Panel>
        ) : (
          <Panel title="反馈">
            <p className="text-sm text-muted-foreground">观察学习路径不记录正向/负向反馈。</p>
          </Panel>
        )}
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

function ClusterScoreList({ clusters }: { clusters: BehaviorRetrievalDebugPayload['matched_clusters'] }) {
  if (clusters.length === 0) return <p className="text-sm text-muted-foreground">暂无数据</p>
  return (
    <div className="space-y-2">
      {clusters.map((cluster) => (
        <div key={cluster.cluster_id} className="rounded-md border px-3 py-2 text-sm">
          <div className="mb-2 flex flex-wrap items-center gap-2">
            <Badge variant="outline">#{cluster.cluster_id}</Badge>
            <span className="text-xs text-muted-foreground">匹配 {formatScore(cluster.score)}</span>
            <span className="text-xs text-muted-foreground">样本 {cluster.source_count}</span>
          </div>
          <p className="mb-2 break-words text-sm font-medium">{clusterTitle(cluster.name, cluster.tags)}</p>
          <ClusterTagPills tags={cluster.tags} maxCount={5} />
        </div>
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
