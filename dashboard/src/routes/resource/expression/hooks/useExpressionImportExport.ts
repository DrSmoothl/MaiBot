/**
 * useExpressionImportExport —— 表达方式「导入 / 清除」领域 hook（页面逻辑下沉切片）。
 *
 * 收编按聊天的导入清除写逻辑：
 * - 导入清除都需要先在左侧选择一个具体聊天（currentChat），未选中时弹 toast 拦截；
 * - 导入：解析上传的 JSON、规范化条目后提交，成功后调用 onChanged() 刷新；
 * - 清除：清空当前聊天全部表达方式，成功后调用 onChanged() 刷新；
 * - 写失败弹全局 toast（与原页面一致）。
 */
import { useCallback } from 'react'
import type { ChangeEvent } from 'react'

import { useToast } from '@/hooks/use-toast'

import { clearExpressions, importExpressions } from '@/lib/expression-api'

import type { ChatInfo, ExpressionExportItem } from '@/types/expression'

export interface UseExpressionImportExportOptions {
  /** 当前选中的具体聊天（按聊天浏览且已选中时非空） */
  currentChat: ChatInfo | null
  /** 写成功后回调（页面接 list.invalidate()），刷新列表 + 统计 */
  onChanged: () => void
  /** 导入 / 清除成功后清空选中集 */
  onClearSelection: () => void
  /** 关闭清除确认对话框 */
  onCloseClearConfirm: () => void
}

export interface UseExpressionImportExportResult {
  /** 导入文件变更处理（绑定到 <input type="file"> 的 onChange） */
  handleImportFileChange: (event: ChangeEvent<HTMLInputElement>) => Promise<void>
  /** 清除当前聊天全部表达方式 */
  clearCurrentChat: () => Promise<void>
}

export function useExpressionImportExport({
  currentChat,
  onChanged,
  onClearSelection,
  onCloseClearConfirm,
}: UseExpressionImportExportOptions): UseExpressionImportExportResult {
  const { toast } = useToast()

  // 校验是否已选择具体聊天，未选中时拦截并提示
  const getCurrentChatId = useCallback((): string | null => {
    if (!currentChat) {
      toast({
        title: '请选择聊天',
        description: '表达方式导入和清除需要先在左侧选择一个具体聊天',
        variant: 'destructive',
      })
      return null
    }
    return currentChat.chat_id
  }, [currentChat, toast])

  // 规范化导入载荷：兼容裸数组与 { expressions: [...] } 两种形态
  const normalizeImportItems = useCallback((payload: unknown): ExpressionExportItem[] => {
    if (Array.isArray(payload)) {
      return payload as ExpressionExportItem[]
    }
    if (
      payload &&
      typeof payload === 'object' &&
      Array.isArray((payload as { expressions?: unknown }).expressions)
    ) {
      return (payload as { expressions: ExpressionExportItem[] }).expressions
    }
    return []
  }, [])

  const handleImportFileChange = useCallback(
    async (event: ChangeEvent<HTMLInputElement>) => {
      const chatId = getCurrentChatId()
      const file = event.target.files?.[0]
      event.target.value = ''
      if (!chatId || !file) return

      try {
        const payload = JSON.parse(await file.text()) as unknown
        const expressionsToImport = normalizeImportItems(payload)
        if (expressionsToImport.length === 0) {
          toast({
            title: '导入失败',
            description: 'JSON 中没有可导入的表达方式',
            variant: 'destructive',
          })
          return
        }

        const result = await importExpressions({
          chat_id: chatId,
          expressions: expressionsToImport,
        })
        toast({
          title: '导入成功',
          description: `成功 ${result.imported_count} 个，跳过 ${result.skipped_count} 个，失败 ${result.failed_count} 个`,
        })
        onChanged()
      } catch (error) {
        toast({
          title: '导入失败',
          description: error instanceof Error ? error.message : '无法解析 JSON 文件',
          variant: 'destructive',
        })
      }
    },
    [getCurrentChatId, normalizeImportItems, onChanged, toast]
  )

  const clearCurrentChat = useCallback(async () => {
    const chatId = getCurrentChatId()
    if (!chatId) return

    let result
    try {
      result = await clearExpressions({ chat_id: chatId })
    } catch (error) {
      toast({
        title: '清除失败',
        description: error instanceof Error ? error.message : '清除表达方式失败',
        variant: 'destructive',
      })
      return
    }

    toast({
      title: '清除成功',
      description: result.message,
    })
    onClearSelection()
    onCloseClearConfirm()
    onChanged()
  }, [getCurrentChatId, onChanged, onClearSelection, onCloseClearConfirm, toast])

  return {
    handleImportFileChange,
    clearCurrentChat,
  }
}
