/**
 * useFeatureStatus —— 功能启用状态领域 hook（页面逻辑下沉）。
 *
 * 收编 index.tsx 的 featureStatus 状态机与 fetchFeatureStatus：
 * 通过 getBotConfigCached / getModelConfigCached 解析记忆（a_memorix.plugin.enabled）
 * 与视觉（model_task_config.vlm.model_list 是否含有效模型）开关。
 */
import { useCallback, useEffect, useRef, useState } from 'react'

import { getBotConfigCached, getModelConfigCached } from '@/lib/config-api'

import type { FeatureStatus } from '../types'

export function useFeatureStatus() {
  const [featureStatus, setFeatureStatus] = useState<FeatureStatus>({
    memoryEnabled: false,
    visualEnabled: false,
  })

  // 使用 ref 跟踪组件是否已卸载，防止内存泄漏
  const isMountedRef = useRef(true)
  useEffect(() => {
    isMountedRef.current = true
    return () => {
      isMountedRef.current = false
    }
  }, [])

  const fetchFeatureStatus = useCallback(async () => {
    try {
      const [botConfigResult, modelConfigResult] = await Promise.all([
        getBotConfigCached(),
        getModelConfigCached(),
      ])

      if (!isMountedRef.current || !botConfigResult.success) return

      const botPayload = botConfigResult.data as { config?: Record<string, unknown> } & Record<string, unknown>
      const botConfig = (botPayload.config ?? botPayload) as Record<string, unknown>
      const memorixConfig = (botConfig.a_memorix ?? {}) as Record<string, unknown>
      const memorixPlugin = (memorixConfig.plugin ?? {}) as Record<string, unknown>

      const modelPayload = modelConfigResult.success
        ? (modelConfigResult.data as { config?: Record<string, unknown> } & Record<string, unknown>)
        : {}
      const modelConfig = (modelPayload.config ?? modelPayload) as Record<string, unknown>
      const taskConfig = (modelConfig.model_task_config ?? {}) as Record<string, unknown>
      const vlmTask = (taskConfig.vlm ?? {}) as Record<string, unknown>
      const vlmModelList = Array.isArray(vlmTask.model_list) ? vlmTask.model_list : []
      const hasVlmModel = vlmModelList.some((modelName) => String(modelName ?? '').trim().length > 0)

      setFeatureStatus({
        memoryEnabled: memorixPlugin.enabled === true,
        visualEnabled: hasVlmModel,
      })
    } catch (error) {
      console.error('获取功能启用状态失败:', error)
      if (isMountedRef.current) {
        setFeatureStatus({
          memoryEnabled: false,
          visualEnabled: false,
        })
      }
    }
  }, [])

  return {
    featureStatus,
    fetchFeatureStatus,
  }
}
