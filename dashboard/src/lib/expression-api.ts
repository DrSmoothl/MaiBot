/**
 * 表达方式管理 API
 */
import { fetchWithAuth } from '@/lib/fetch-with-auth'
import { formatApiError } from '@/lib/api-error'
import type {
  BatchReviewItem,
  BatchReviewResponse,
  ChatInfo,
  ChatListResponse,
  Expression,
  ExpressionCreateRequest,
  ExpressionCreateResponse,
  ExpressionDeleteResponse,
  ExpressionDetailResponse,
  ExpressionClearResponse,
  ExpressionExportItem,
  ExpressionExportResponse,
  ExpressionImportResponse,
  ExpressionGroupListResponse,
  ExpressionListResponse,
  ExpressionReviewLogApproveResponse,
  ExpressionReviewLogListResponse,
  ExpressionStatsResponse,
  ExpressionUpdateRequest,
  ExpressionUpdateResponse,
  LegacyExpressionImportPreviewResponse,
  LegacyExpressionImportResponse,
  ReviewListResponse,
  ReviewStats,
} from '@/types/expression'
import type { ApiResponse } from '@/types/api'

const API_BASE = '/api/webui/expression'

/**
 * 获取聊天列表
 */
export async function getChatList(
  params: { include_legacy?: boolean } = {}
): Promise<ApiResponse<ChatInfo[]>> {
  const queryParams = new URLSearchParams()
  if (params.include_legacy) queryParams.append('include_legacy', 'true')
  const response = await fetchWithAuth(`${API_BASE}/chats?${queryParams}`, {})

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取聊天列表失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取聊天列表失败',
      }
    }
  }

  try {
    const data: ChatListResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: data.data,
      }
    } else {
      return {
        success: false,
        error: '获取聊天列表失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析聊天列表响应',
    }
  }
}

/**
 * 获取可作为导入目标的全部聊天流。
 */
export async function getExpressionChatTargets(
  params: { include_legacy?: boolean } = {}
): Promise<ApiResponse<ChatInfo[]>> {
  const queryParams = new URLSearchParams()
  if (params.include_legacy) queryParams.append('include_legacy', 'true')
  const response = await fetchWithAuth(`${API_BASE}/chat-targets?${queryParams}`, {})

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取导入目标聊天流失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取导入目标聊天流失败',
      }
    }
  }

  try {
    const data: ChatListResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: data.data,
      }
    }
    return {
      success: false,
      error: '获取导入目标聊天流失败',
    }
  } catch {
    return {
      success: false,
      error: '无法解析导入目标聊天流响应',
    }
  }
}

/**
 * 获取表达互通组列表
 */
export async function getExpressionGroups(
  params: { include_legacy?: boolean } = {}
): Promise<ApiResponse<ExpressionGroupListResponse['data']>> {
  const queryParams = new URLSearchParams()
  if (params.include_legacy) queryParams.append('include_legacy', 'true')
  const response = await fetchWithAuth(`${API_BASE}/groups?${queryParams}`, {})

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取表达互通组失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取表达互通组失败',
      }
    }
  }

  try {
    const data: ExpressionGroupListResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: data.data,
      }
    }
    return {
      success: false,
      error: '获取表达互通组失败',
    }
  } catch {
    return {
      success: false,
      error: '无法解析表达互通组响应',
    }
  }
}

/**
 * 获取表达方式列表
 */
export async function getExpressionList(params: {
  page?: number
  page_size?: number
  search?: string
  chat_id?: string
  chat_ids?: string[]
  include_legacy?: boolean
  review_filter?: 'all' | 'user_checked' | 'unchecked'
  sort_by?: 'time'
}): Promise<ApiResponse<ExpressionListResponse>> {
  const queryParams = new URLSearchParams()

  if (params.page) queryParams.append('page', params.page.toString())
  if (params.page_size) queryParams.append('page_size', params.page_size.toString())
  if (params.search) queryParams.append('search', params.search)
  if (params.chat_id) queryParams.append('chat_id', params.chat_id)
  if (params.include_legacy) queryParams.append('include_legacy', 'true')
  if (params.review_filter) queryParams.append('review_filter', params.review_filter)
  if (params.sort_by) queryParams.append('sort_by', params.sort_by)
  params.chat_ids?.forEach((chatId) => queryParams.append('chat_ids', chatId))

  const response = await fetchWithAuth(`${API_BASE}/list?${queryParams}`, {})

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取表达方式列表失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取表达方式列表失败',
      }
    }
  }

  try {
    const data: ExpressionListResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: data,
      }
    } else {
      return {
        success: false,
        error: '获取表达方式列表失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析表达方式列表响应',
    }
  }
}

/**
 * 按聊天导出表达方式。导出的 JSON 不包含 session_id。
 */
export async function exportExpressions(params: {
  chat_id: string
  ids?: number[]
}): Promise<ApiResponse<ExpressionExportResponse>> {
  const response = await fetchWithAuth(`${API_BASE}/export`, {
    method: 'POST',
    body: JSON.stringify(params),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '导出表达方式失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '导出表达方式失败',
      }
    }
  }

  try {
    const data: ExpressionExportResponse = await response.json()
    return {
      success: true,
      data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析表达方式导出响应',
    }
  }
}

/**
 * 将表达方式 JSON 导入到指定聊天。
 */
export async function importExpressions(params: {
  chat_id: string
  expressions: ExpressionExportItem[]
}): Promise<ApiResponse<ExpressionImportResponse>> {
  const response = await fetchWithAuth(`${API_BASE}/import`, {
    method: 'POST',
    body: JSON.stringify(params),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '导入表达方式失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '导入表达方式失败',
      }
    }
  }

  try {
    const data: ExpressionImportResponse = await response.json()
    return {
      success: true,
      data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析表达方式导入响应',
    }
  }
}

/**
 * 清除指定聊天下的全部表达方式。
 */
export async function clearExpressions(params: {
  chat_id: string
}): Promise<ApiResponse<ExpressionClearResponse>> {
  const response = await fetchWithAuth(`${API_BASE}/clear`, {
    method: 'POST',
    body: JSON.stringify(params),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '清除表达方式失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '清除表达方式失败',
      }
    }
  }

  try {
    const data: ExpressionClearResponse = await response.json()
    return {
      success: true,
      data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析表达方式清除响应',
    }
  }
}

/**
 * 预览旧版数据库表达方式导入。
 */
export async function previewLegacyExpressionImport(params: {
  db_path: string
}): Promise<ApiResponse<LegacyExpressionImportPreviewResponse>> {
  const response = await fetchWithAuth(`${API_BASE}/legacy-import/preview`, {
    method: 'POST',
    body: JSON.stringify(params),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '预览旧版导入失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '预览旧版导入失败',
      }
    }
  }

  try {
    const data: LegacyExpressionImportPreviewResponse = await response.json()
    return {
      success: true,
      data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析旧版导入预览响应',
    }
  }
}

/**
 * 上传旧版数据库并预览表达方式导入。
 */
export async function previewLegacyExpressionImportFile(
  file: File
): Promise<ApiResponse<LegacyExpressionImportPreviewResponse>> {
  const formData = new FormData()
  formData.append('file', file)
  const response = await fetchWithAuth(`${API_BASE}/legacy-import/preview-file`, {
    method: 'POST',
    body: formData,
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '预览旧版导入失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '预览旧版导入失败',
      }
    }
  }

  try {
    const data: LegacyExpressionImportPreviewResponse = await response.json()
    return {
      success: true,
      data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析旧版导入预览响应',
    }
  }
}

/**
 * 按映射从旧版数据库导入表达方式。
 */
export async function importLegacyExpressions(params: {
  db_path: string
  mappings: Array<{
    old_chat_id: string
    target_chat_id?: string | null
    target_chat_ids?: string[]
  }>
}): Promise<ApiResponse<LegacyExpressionImportResponse>> {
  const response = await fetchWithAuth(`${API_BASE}/legacy-import/import`, {
    method: 'POST',
    body: JSON.stringify(params),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '旧版导入失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '旧版导入失败',
      }
    }
  }

  try {
    const data: LegacyExpressionImportResponse = await response.json()
    return {
      success: true,
      data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析旧版导入响应',
    }
  }
}

/**
 * 获取表达方式详细信息
 */
export async function getExpressionDetail(expressionId: number): Promise<ApiResponse<any>> {
  const response = await fetchWithAuth(`${API_BASE}/${expressionId}`, {})

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取表达方式详情失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取表达方式详情失败',
      }
    }
  }

  try {
    const data: ExpressionDetailResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: data.data,
      }
    } else {
      return {
        success: false,
        error: '获取表达方式详情失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析表达方式详情响应',
    }
  }
}

/**
 * 创建表达方式
 */
export async function createExpression(data: ExpressionCreateRequest): Promise<ApiResponse<any>> {
  const response = await fetchWithAuth(`${API_BASE}/`, {
    method: 'POST',

    body: JSON.stringify(data),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '创建表达方式失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '创建表达方式失败',
      }
    }
  }

  try {
    const responseData: ExpressionCreateResponse = await response.json()
    if (responseData.success) {
      return {
        success: true,
        data: responseData.data,
      }
    } else {
      return {
        success: false,
        error: responseData.message || '创建表达方式失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析创建表达方式响应',
    }
  }
}

/**
 * 更新表达方式（增量更新）
 */
export async function updateExpression(
  expressionId: number,
  data: ExpressionUpdateRequest
): Promise<ApiResponse<any>> {
  const response = await fetchWithAuth(`${API_BASE}/${expressionId}`, {
    method: 'PATCH',

    body: JSON.stringify(data),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '更新表达方式失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '更新表达方式失败',
      }
    }
  }

  try {
    const responseData: ExpressionUpdateResponse = await response.json()
    if (responseData.success) {
      return {
        success: true,
        data: responseData.data || {},
      }
    } else {
      return {
        success: false,
        error: responseData.message || '更新表达方式失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析更新表达方式响应',
    }
  }
}

/**
 * 删除表达方式
 */
export async function updateExpressionReviewStatus(
  expressionId: number,
  approved: boolean
): Promise<ApiResponse<Expression>> {
  const response = await fetchWithAuth(`${API_BASE}/${expressionId}/review-status`, {
    method: 'PATCH',
    body: JSON.stringify({ approved }),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '更新表达方式审核状态失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '更新表达方式审核状态失败',
      }
    }
  }

  try {
    const responseData: ExpressionUpdateResponse = await response.json()
    if (responseData.success && responseData.data) {
      return {
        success: true,
        data: responseData.data,
      }
    }
    return {
      success: false,
      error: responseData.message || '更新表达方式审核状态失败',
    }
  } catch {
    return {
      success: false,
      error: '无法解析表达方式审核状态响应',
    }
  }
}

export async function deleteExpression(expressionId: number): Promise<ApiResponse<any>> {
  const response = await fetchWithAuth(`${API_BASE}/${expressionId}`, {
    method: 'DELETE',
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '删除表达方式失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '删除表达方式失败',
      }
    }
  }

  try {
    const data: ExpressionDeleteResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: {},
      }
    } else {
      return {
        success: false,
        error: data.message || '删除表达方式失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析删除表达方式响应',
    }
  }
}

/**
 * 批量删除表达方式
 */
export async function batchDeleteExpressions(expressionIds: number[]): Promise<ApiResponse<any>> {
  const response = await fetchWithAuth(`${API_BASE}/batch/delete`, {
    method: 'POST',

    body: JSON.stringify({ ids: expressionIds }),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '批量删除表达方式失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '批量删除表达方式失败',
      }
    }
  }

  try {
    const data: ExpressionDeleteResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: {},
      }
    } else {
      return {
        success: false,
        error: data.message || '批量删除表达方式失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析批量删除表达方式响应',
    }
  }
}

/**
 * 获取表达方式统计数据
 */
export async function getExpressionStats(
  params: { include_legacy?: boolean } = {}
): Promise<ApiResponse<any>> {
  const queryParams = new URLSearchParams()
  if (params.include_legacy) queryParams.append('include_legacy', 'true')
  const response = await fetchWithAuth(`${API_BASE}/stats/summary?${queryParams}`, {})

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取统计数据失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取统计数据失败',
      }
    }
  }

  try {
    const data: ExpressionStatsResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: data.data,
      }
    } else {
      return {
        success: false,
        error: '获取统计数据失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析统计数据响应',
    }
  }
}

// ============ 审核相关 API ============

/**
 * 获取审核统计数据
 */
export async function getReviewStats(): Promise<ApiResponse<ReviewStats>> {
  const response = await fetchWithAuth(`${API_BASE}/review/stats`)

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取审核统计失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取审核统计失败',
      }
    }
  }

  try {
    const data = (await response.json()) as ReviewStats
    return {
      success: true,
      data: data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析审核统计响应',
    }
  }
}

/**
 * 获取审核列表
 */
export async function getReviewList(params: {
  page?: number
  page_size?: number
  filter_type?: 'unchecked' | 'passed' | 'all'
  order?: 'latest' | 'random'
  search?: string
  chat_id?: string
  exclude_ids?: number[]
}): Promise<ApiResponse<ReviewListResponse>> {
  const queryParams = new URLSearchParams()

  if (params.page) queryParams.append('page', params.page.toString())
  if (params.page_size) queryParams.append('page_size', params.page_size.toString())
  if (params.filter_type) queryParams.append('filter_type', params.filter_type)
  if (params.order) queryParams.append('order', params.order)
  if (params.search) queryParams.append('search', params.search)
  if (params.chat_id) queryParams.append('chat_id', params.chat_id)
  params.exclude_ids?.forEach((id) => queryParams.append('exclude_ids', id.toString()))

  const response = await fetchWithAuth(`${API_BASE}/review/list?${queryParams}`)

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取审核列表失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取审核列表失败',
      }
    }
  }

  try {
    const data: ReviewListResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: data,
      }
    } else {
      return {
        success: false,
        error: '获取审核列表失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析审核列表响应',
    }
  }
}

/**
 * 批量审核表达方式
 */
export async function batchReviewExpressions(
  items: BatchReviewItem[]
): Promise<ApiResponse<BatchReviewResponse>> {
  const response = await fetchWithAuth(`${API_BASE}/review/batch`, {
    method: 'POST',
    body: JSON.stringify({ items }),
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '批量审核失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '批量审核失败',
      }
    }
  }

  try {
    const data: BatchReviewResponse = await response.json()
    if (data.success) {
      return {
        success: true,
        data: data,
      }
    } else {
      return {
        success: false,
        error: '批量审核失败',
      }
    }
  } catch {
    return {
      success: false,
      error: '无法解析批量审核响应',
    }
  }
}

export async function getExpressionReviewLogs(
  params: {
    limit?: number
    passed?: boolean
    chat_id?: string
  } = {}
): Promise<ApiResponse<ExpressionReviewLogListResponse>> {
  const queryParams = new URLSearchParams()
  if (params.limit) queryParams.append('limit', params.limit.toString())
  if (params.passed !== undefined) queryParams.append('passed', params.passed ? 'true' : 'false')
  if (params.chat_id) queryParams.append('chat_id', params.chat_id)

  const response = await fetchWithAuth(`${API_BASE}/review/logs?${queryParams}`)

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '获取 AI 审核记录失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '获取 AI 审核记录失败',
      }
    }
  }

  try {
    const data: ExpressionReviewLogListResponse = await response.json()
    return {
      success: true,
      data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析 AI 审核记录响应',
    }
  }
}

export async function approveExpressionReviewLog(
  reviewLogId: string
): Promise<ApiResponse<ExpressionReviewLogApproveResponse>> {
  const response = await fetchWithAuth(`${API_BASE}/review/logs/${reviewLogId}/approve`, {
    method: 'POST',
  })

  if (!response.ok) {
    try {
      const errorData = await response.json()
      return {
        success: false,
        error: formatApiError(errorData, '恢复表达方式失败'),
      }
    } catch {
      return {
        success: false,
        error: response.statusText || '恢复表达方式失败',
      }
    }
  }

  try {
    const data: ExpressionReviewLogApproveResponse = await response.json()
    return {
      success: true,
      data,
    }
  } catch {
    return {
      success: false,
      error: '无法解析恢复表达方式响应',
    }
  }
}
