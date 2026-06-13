/**
 * 迁移期兼容层：把 throw ApiError 契约包装回 ApiResponse<T> 判别联合。
 *
 * 仅供尚未切换到 throw 契约的旧调用方（页面层）使用；
 * 待数据获取 hook 落地、页面层统一消费 ApiError 后整体移除。
 */
import type { ApiResponse } from '@/types/api'

import { ApiError } from './errors'

/**
 * 执行一个请求流程，把 ApiError 收敛为 { success: false, error }。
 * 非 ApiError 的异常（编程错误）原样抛出，不做掩盖。
 */
export async function toApiResponse<T>(run: () => Promise<T>): Promise<ApiResponse<T>> {
  try {
    return { success: true, data: await run() }
  } catch (error) {
    if (error instanceof ApiError) {
      return { success: false, error: error.message }
    }
    throw error
  }
}

/**
 * 迁移期桥接：把仍返回 ApiResponse 的（被多页共享、暂不能切 throw 契约的）API
 * 解包成「成功取数据、失败抛 ApiError」，以便直接用作 TanStack Query 的 queryFn。
 *
 * 用法：`queryFn: () => getModelConfig().then(unwrapApiResponse)`
 * 待该 API 的最后一个使用者迁移完成、API 自身切换 throw 契约后即可移除。
 */
export function unwrapApiResponse<T>(response: ApiResponse<T>): T {
  if (response.success) {
    return response.data
  }
  throw new ApiError(response.error)
}
