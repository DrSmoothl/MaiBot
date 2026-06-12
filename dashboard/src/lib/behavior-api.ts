import { fetchWithAuth } from './fetch-with-auth'

const API_BASE = '/api/webui/behavior'

export interface BehaviorChatInfo {
  session_id: string
  display_name: string
  platform: string
  chat_type: string
  path_count: number
  cluster_count: number
  scene_count: number
  last_active_time: string | null
}

export interface BehaviorClusterTag {
  tag: string
  probability: number
  display?: string
}

export interface BehaviorSceneCluster {
  id: number | null
  name: string
  tags: BehaviorClusterTag[]
  source_count: number
  score: number
  update_time: string | null
}

export interface BehaviorClusterItem extends BehaviorSceneCluster {
  session_id: string | null
  chat_name: string
  path_count: number
  enabled_path_count: number
  activation_count: number
  success_count: number
  failure_count: number
  observed_path_count: number
  self_reflection_path_count: number
  last_active_time: string | null
}

export interface BehaviorPathItem {
  id: number
  session_id: string | null
  chat_name: string
  trigger: string
  scene_cluster_id: number | null
  scene_cluster_name: string
  scene_cluster_tags: BehaviorClusterTag[]
  scene_cluster_source_count: number
  scene_cluster_score: number
  actor_type: string
  learning_type: string
  action: string
  outcome: string
  count: number
  activation_count: number
  success_count: number
  failure_count: number
  score: number
  enabled: boolean
  last_active_time: string | null
  last_feedback_time: string | null
  update_time: string | null
}

export interface BehaviorPathListResponse {
  success: boolean
  total: number
  page: number
  page_size: number
  data: BehaviorPathItem[]
}

export interface BehaviorClusterListResponse {
  success: boolean
  total: number
  page: number
  page_size: number
  data: BehaviorClusterItem[]
}

export interface BehaviorReadableTag {
  tag: string
  kind: string
  cluster_key: string
  display: string
  probability: number
}

export interface BehaviorSceneGraphNode {
  id: number
  label: string
  short_label: string
  session_id: string
  source_count: number
  score: number
  path_count: number
  activation_count: number
  success_count: number
  failure_count: number
  update_time: string | null
  tags: BehaviorReadableTag[]
}

export interface BehaviorSceneGraphEdge {
  source: number
  target: number
  source_label: string
  target_label: string
  weight: number
  shared_tags: Array<{
    tag: string
    display: string
    left: number
    right: number
    overlap: number
  }>
}

export interface BehaviorTagNetworkNode {
  id: string
  kind: string
  cluster_key: string
  label: string
  aliases: string[]
  weight: number
  scene_count: number
  source_count: number
}

export interface BehaviorTagNetworkEdge {
  source: string
  target: string
  weight: number
  count: number
}

export interface BehaviorGraphData {
  scene_cluster_network: {
    nodes: BehaviorSceneGraphNode[]
    edges: BehaviorSceneGraphEdge[]
  }
  tag_network: {
    nodes: BehaviorTagNetworkNode[]
    edges: BehaviorTagNetworkEdge[]
  }
}

export interface BehaviorGraphNode {
  id: number
  kind: string
  label: string
  score: number
  source_count: number
}

export interface BehaviorGraphEdge {
  id: string
  source: string
  target: string
  kind: string
  weight: number
  count: number
}

export interface BehaviorPathDetail {
  path: BehaviorPathItem
  scene_cluster: BehaviorSceneCluster
  evidence: unknown[]
  feedback: unknown[]
  nodes: BehaviorGraphNode[]
  edges: BehaviorGraphEdge[]
}

export interface BehaviorDescriptor {
  node_kind: string
  name: string
  weight: number
}

export interface BehaviorMatchedCluster {
  cluster_id: number
  name: string
  score: number
  tags: BehaviorClusterTag[]
  source_count: number
  cluster_score: number
}

export interface BehaviorRetrievalCandidate {
  behavior_id: number
  score: number
  path: BehaviorPathItem | null
}

export interface BehaviorRetrievalDebugStage {
  direct_tag_count: number
  expanded_tag_count?: number
  hop_counts?: Record<string, number>
  total_query_tag_count?: number
  cluster_count: number
}

export interface BehaviorRetrievalDebugInfo {
  direct?: BehaviorRetrievalDebugStage
  spread?: BehaviorRetrievalDebugStage
  direct_top_score?: number
  direct_locked?: boolean
  direct_lock_threshold?: number
  locked_direct_spread_factor?: number
}

export interface BehaviorRetrievalDebugPayload {
  retrieval_mode: string
  descriptors: BehaviorDescriptor[]
  matched_clusters: BehaviorMatchedCluster[]
  candidate_scores: Array<{ behavior_id: number; score: number }>
  candidates: BehaviorRetrievalCandidate[]
  retrieval_debug: BehaviorRetrievalDebugInfo
  error?: string
}

export interface BehaviorRetrievalDebugRequest {
  session_id?: string
  include_global: boolean
  retrieval_mode?: string
  summary?: string
  tag_clusters: Array<{ tag_name: string; tag_aliases: string[] }>
  need: { tag_name: string; tag_aliases: string[] }
  other_traits: Array<{ tag_name: string; tag_aliases: string[] }>
  max_count: number
}

async function readJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || `请求失败：${response.status}`)
  }
  return response.json() as Promise<T>
}

export async function listBehaviorChats(): Promise<{ success: boolean; data: BehaviorChatInfo[] }> {
  const response = await fetchWithAuth(`${API_BASE}/chats`)
  return readJson(response)
}

export async function listBehaviorPaths(params: {
  session_id?: string
  search?: string
  enabled?: string
  actor_type?: string
  learning_type?: string
  sort_by?: string
  sort_order?: string
  page?: number
  page_size?: number
}): Promise<BehaviorPathListResponse> {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== '') query.set(key, String(value))
  })
  const response = await fetchWithAuth(`${API_BASE}/paths?${query.toString()}`)
  return readJson(response)
}

export async function listBehaviorClusters(params: {
  session_id?: string
  search?: string
  page?: number
  page_size?: number
}): Promise<BehaviorClusterListResponse> {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== '') query.set(key, String(value))
  })
  const response = await fetchWithAuth(`${API_BASE}/clusters?${query.toString()}`)
  return readJson(response)
}

export async function getBehaviorGraphData(params: {
  session_id?: string
} = {}): Promise<{ success: boolean; data: BehaviorGraphData }> {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== '') query.set(key, String(value))
  })
  const suffix = query.toString() ? `?${query.toString()}` : ''
  const response = await fetchWithAuth(`${API_BASE}/graph-data${suffix}`)
  return readJson(response)
}

export async function getBehaviorPathDetail(pathId: number): Promise<{ success: boolean; data: BehaviorPathDetail }> {
  const response = await fetchWithAuth(`${API_BASE}/paths/${pathId}`)
  return readJson(response)
}

export async function debugBehaviorRetrieval(
  payload: BehaviorRetrievalDebugRequest
): Promise<{ success: boolean; data: BehaviorRetrievalDebugPayload }> {
  const response = await fetchWithAuth(`${API_BASE}/retrieval-debug`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return readJson(response)
}
