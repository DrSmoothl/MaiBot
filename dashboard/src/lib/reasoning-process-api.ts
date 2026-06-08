import { parseResponse, throwIfError } from '@/lib/api-helpers'
import { resolveApiPath } from '@/lib/api-base'
import { fetchWithAuth } from '@/lib/fetch-with-auth'

const API_BASE = '/api/webui/reasoning-process'

export type ReasoningPromptFile = {
  stage: string
  session_id: string
  resolved_session_id: string | null
  session_display_name: string | null
  platform: string | null
  chat_type: string | null
  target_id: string | null
  stem: string
  timestamp: number | null
  text_path: string | null
  html_path: string | null
  json_path: string | null
  output_preview: string | null
  action_preview: string | null
  model_name: string | null
  duration_ms: number | null
  size: number
  modified_at: number
}

export type ReasoningPromptStageInfo = {
  name: string
  session_count: number
  latest_modified_at: number
}

export type ReasoningPromptSessionInfo = {
  name: string
  platform: string
  chat_type: string
  target_id: string
  resolved_session_id: string | null
  display_name: string
  account_id: string | null
  matched_current_account: boolean
}

export type ReasoningPromptListResponse = {
  items: ReasoningPromptFile[]
  total: number
  page: number
  page_size: number
  stages: string[]
  stage_infos: ReasoningPromptStageInfo[]
  sessions: string[]
  session_infos: ReasoningPromptSessionInfo[]
  selected_session: string
}

export type ReasoningPromptStagesResponse = {
  stages: string[]
  stage_infos: ReasoningPromptStageInfo[]
}

export type ReasoningPromptContentResponse = {
  path: string
  content: string
  size: number
  modified_at: number
  model_name: string | null
  duration_ms: number | null
}

export type ReasoningPromptListParams = {
  stage?: string
  session?: string
  search?: string
  page?: number
  pageSize?: number
}

export async function listReasoningPromptFiles(
  params: ReasoningPromptListParams
): Promise<ReasoningPromptListResponse> {
  const queryParams = new URLSearchParams()
  queryParams.set('stage', params.stage ?? 'planner')
  queryParams.set('session', params.session ?? 'auto')
  queryParams.set('search', params.search ?? '')
  queryParams.set('page', String(params.page ?? 1))
  queryParams.set('page_size', String(params.pageSize ?? 50))

  const response = await fetchWithAuth(`${API_BASE}/files?${queryParams}`, { cache: 'no-store' })
  return throwIfError(await parseResponse<ReasoningPromptListResponse>(response))
}

export async function listReasoningPromptStages(): Promise<ReasoningPromptStagesResponse> {
  const response = await fetchWithAuth(`${API_BASE}/stages`, { cache: 'no-store' })
  return throwIfError(await parseResponse<ReasoningPromptStagesResponse>(response))
}

export async function getReasoningPromptFile(
  path: string
): Promise<ReasoningPromptContentResponse> {
  const response = await fetchWithAuth(`${API_BASE}/file?path=${encodeURIComponent(path)}`, {
    cache: 'no-store',
  })
  return throwIfError(await parseResponse<ReasoningPromptContentResponse>(response))
}

export async function getReasoningPromptHtmlUrl(path: string): Promise<string> {
  return resolveApiPath(`${API_BASE}/html?path=${encodeURIComponent(path)}`)
}
