/**
 * 观察聊天流的适配器放行状态查询。
 *
 * 聊天页侧边栏据此把「麦麦的聊天流」拆成适配器允许（仍在处理消息）与
 * 适配器不允许（被适配器规则阻止，实际不活跃）两组。
 */
import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'

import {
  CHAT_ADAPTER_STATUS_QUERY_KEY,
  getChatSessionsAdapterStatus,
  type SessionAdapterStatus,
} from '@/lib/chat-management-api'

const EMPTY_STATUSES: Map<string, SessionAdapterStatus> = new Map()

export function useObservedAdapterStatuses(sessionIds: string[]): Map<string, SessionAdapterStatus> {
  // 会话集合顺序不稳定，先排序去重得到稳定的查询键，避免无意义地重复请求
  const sortedSessionIds = useMemo(
    () => Array.from(new Set(sessionIds.filter(Boolean))).sort(),
    [sessionIds]
  )
  const sessionIdKey = sortedSessionIds.join(',')

  const statusQuery = useQuery({
    queryKey: [CHAT_ADAPTER_STATUS_QUERY_KEY, sessionIdKey],
    queryFn: () => getChatSessionsAdapterStatus(sessionIdKey ? sessionIdKey.split(',') : []),
    enabled: sessionIdKey.length > 0,
  })

  return useMemo(
    () => (statusQuery.data ? new Map(Object.entries(statusQuery.data)) : EMPTY_STATUSES),
    [statusQuery.data]
  )
}
