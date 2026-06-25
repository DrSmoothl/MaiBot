import { useCallback, useEffect, useRef } from 'react'

import { updateBotConfigSection } from '@/lib/config-api'

/**
 * Bot 配置页自动保存配置。
 */
export interface UseAutoSaveOptions {
  /** Debounce delay in milliseconds, default 2000ms */
  debounceMs?: number
  /** Save success callback */
  onSaveSuccess?: () => void
  /** Save error callback */
  onSaveError?: (error: Error) => void
}

export interface UseAutoSaveReturn {
  /** Trigger auto-save */
  triggerAutoSave: (sectionName: string, sectionData: unknown) => void
  /** Save immediately */
  saveNow: (sectionName: string, sectionData: unknown) => Promise<void>
  /** Cancel pending auto-save */
  cancelPendingAutoSave: () => void
}

/**
 * Bot 配置页自动保存 hook。
 */
export function useAutoSave(
  isInitialLoad: boolean,
  setAutoSaving: (saving: boolean) => void,
  setHasUnsavedChanges: (hasChanges: boolean) => void,
  options: UseAutoSaveOptions = {}
): UseAutoSaveReturn {
  const { debounceMs = 2000, onSaveSuccess, onSaveError } = options
  const autoSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Execute save operation
  const saveSection = useCallback(
    async (sectionName: string, sectionData: unknown) => {
      try {
        setAutoSaving(true)
        await updateBotConfigSection(sectionName, sectionData)
        setHasUnsavedChanges(false)
        onSaveSuccess?.()
      } catch (error) {
        console.error(`自动保存 ${sectionName} 失败:`, error)
        setHasUnsavedChanges(true)
        onSaveError?.(error instanceof Error ? error : new Error(String(error)))
      } finally {
        setAutoSaving(false)
      }
    },
    [setAutoSaving, setHasUnsavedChanges, onSaveSuccess, onSaveError]
  )

  // Trigger auto-save (with debounce)
  const triggerAutoSave = useCallback(
    (sectionName: string, sectionData: unknown) => {
      if (isInitialLoad) return

      setHasUnsavedChanges(true)

      if (autoSaveTimerRef.current) {
        clearTimeout(autoSaveTimerRef.current)
      }

      autoSaveTimerRef.current = setTimeout(() => {
        saveSection(sectionName, sectionData)
      }, debounceMs)
    },
    [isInitialLoad, setHasUnsavedChanges, saveSection, debounceMs]
  )

  // Save immediately (no debounce)
  const saveNow = useCallback(
    async (sectionName: string, sectionData: unknown) => {
      if (autoSaveTimerRef.current) {
        clearTimeout(autoSaveTimerRef.current)
        autoSaveTimerRef.current = null
      }
      await saveSection(sectionName, sectionData)
    },
    [saveSection]
  )

  // Cancel pending auto-save
  const cancelPendingAutoSave = useCallback(() => {
    if (autoSaveTimerRef.current) {
      clearTimeout(autoSaveTimerRef.current)
      autoSaveTimerRef.current = null
    }
  }, [])

  // Cleanup timer on unmount
  useEffect(() => {
    return () => {
      if (autoSaveTimerRef.current) {
        clearTimeout(autoSaveTimerRef.current)
      }
    }
  }, [])

  return {
    triggerAutoSave,
    saveNow,
    cancelPendingAutoSave,
  }
}

