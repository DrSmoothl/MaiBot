import type { ChatStreamType } from '@/lib/chat-management-api'

/** 格式化聊天流所属的 Bot 平台账号，用于区分同名聊天流。 */
export function formatChatAccountLabel(accountId?: string | null): string {
  const normalizedAccountId = accountId?.trim()
  return normalizedAccountId ? `账号 ${normalizedAccountId}` : ''
}

/** 在聊天流名称后附加 Bot 平台账号；旧数据缺少账号时保持原名称。 */
export function formatChatDisplayName(name: string, accountId?: string | null): string {
  const accountLabel = formatChatAccountLabel(accountId)
  return accountLabel ? `${name} · ${accountLabel}` : name
}

/** 把聊天流类型转换为中文标签。 */
export function getChatTypeText(chatType: ChatStreamType): string {
  return chatType === 'group' ? '群聊' : '私聊'
}
