import { backendApi } from '@/lib/http'

export type ChatStreamType = 'group' | 'private'

export interface ChatStream {
  id: number | null
  session_id: string
  display_name: string
  chat_type: ChatStreamType
  target_id: string
  platform: string
  account_id: string | null
  scope: string | null
  user_id: string | null
  user_nickname: string | null
  user_cardname: string | null
  group_id: string | null
  group_name: string | null
  message_count: number
  created_at: number | null
  last_active_at: number | null
  latest_message: string
  latest_message_at: number | null
}

export interface ChatConfigRule {
  platform: string
  item_id: string
  type: ChatStreamType | string
  use?: boolean
  learn?: boolean
  is_default?: boolean
  is_wildcard?: boolean
}

export interface ChatLearningStatus {
  use: boolean
  learn: boolean
  matched_rule: ChatConfigRule | null
}

export interface ChatTalkFrequencyRule {
  platform: string
  item_id: string
  type: ChatStreamType | string
  time: string
  value: number
  value_label: string
  target_priority: number
  time_priority: number | null
  time_active: boolean
  is_effective: boolean
  is_default_target: boolean
}

export interface ChatTalkFrequencyDetail {
  enabled: boolean
  base_value: number
  base_value_label: string
  effective_value: number
  effective_value_label: string
  current_time: string
  matched_rules: ChatTalkFrequencyRule[]
}

export interface ChatStreamDetail {
  session_id: string
  display_name: string
  chat_type: ChatStreamType
  platform: string
  target_id: string
  group_id: string | null
  user_id: string | null
  expression: ChatLearningStatus
  behavior?: ChatLearningStatus
  jargon: ChatLearningStatus
  talk_frequency: ChatTalkFrequencyDetail
}

interface ChatStreamsResponse {
  success: boolean
  sessions?: ChatStream[]
  total?: number
}

interface ChatStreamDetailResponse {
  success: boolean
  detail?: ChatStreamDetail
}

interface UpdateTalkFrequencyPayload {
  previous_time?: string | null
  time: string
  value: number
}

export async function getChatStreams(limit = 1000): Promise<ChatStream[]> {
  const result = await backendApi.get<ChatStreamsResponse>('/api/chat/sessions', {
    query: { limit },
  })
  return result.sessions ?? []
}

export async function getChatStreamDetail(sessionId: string): Promise<ChatStreamDetail> {
  const result = await backendApi.get<ChatStreamDetailResponse>(
    `/api/chat/sessions/${encodeURIComponent(sessionId)}`
  )
  if (!result.detail) {
    throw new Error('聊天流详情为空')
  }
  return result.detail
}

export async function updateChatStreamTalkFrequency(
  sessionId: string,
  payload: UpdateTalkFrequencyPayload
): Promise<ChatStreamDetail> {
  const result = await backendApi.put<ChatStreamDetailResponse>(
    `/api/chat/sessions/${encodeURIComponent(sessionId)}/talk-frequency`,
    {
      body: payload,
      errorMessage: '保存发言频率失败',
    }
  )
  if (!result.detail) {
    throw new Error('聊天流详情为空')
  }
  return result.detail
}

export async function deleteChatStreamTalkFrequency(
  sessionId: string,
  time: string
): Promise<ChatStreamDetail> {
  const result = await backendApi.delete<ChatStreamDetailResponse>(
    `/api/chat/sessions/${encodeURIComponent(sessionId)}/talk-frequency`,
    {
      query: { time },
      errorMessage: '删除发言频率规则失败',
    }
  )
  if (!result.detail) {
    throw new Error('聊天流详情为空')
  }
  return result.detail
}
