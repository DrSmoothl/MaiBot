import { useEffect, useMemo, useState } from 'react'

import { resolveApiPath } from '@/lib/api-base'
import { getSetting } from '@/lib/settings-manager'

export type AvatarTargetType = 'user' | 'group'

export function isAvatarFetchEnabled(): boolean {
  return getSetting('enableAvatarFetch')
}

export function useAvatarFetchEnabled(): boolean {
  const [enabled, setEnabled] = useState(() => isAvatarFetchEnabled())

  useEffect(() => {
    const syncEnabled = () => setEnabled(isAvatarFetchEnabled())
    const handleSettingsChange = (event: Event) => {
      const detail = (event as CustomEvent<{ key?: string }>).detail
      if (!detail?.key || detail.key === 'enableAvatarFetch') {
        syncEnabled()
      }
    }

    window.addEventListener('maibot-settings-change', handleSettingsChange)
    window.addEventListener('maibot-settings-reset', syncEnabled)
    window.addEventListener('storage', syncEnabled)
    return () => {
      window.removeEventListener('maibot-settings-change', handleSettingsChange)
      window.removeEventListener('maibot-settings-reset', syncEnabled)
      window.removeEventListener('storage', syncEnabled)
    }
  }, [])

  return enabled
}

export function buildWebuiAvatarPath(
  platform?: string | null,
  targetId?: string | null,
  targetType: AvatarTargetType = 'user'
): string | null {
  const normalizedPlatform = String(platform || '').trim().toLowerCase()
  const normalizedTargetId = String(targetId || '').trim()
  if (!normalizedPlatform || !normalizedTargetId) return null
  const idParam = targetType === 'group' ? 'group_id' : 'user_id'
  return `/api/webui/avatar?platform=${encodeURIComponent(normalizedPlatform)}&${idParam}=${encodeURIComponent(normalizedTargetId)}`
}

export function useResolvedAvatarUrl(
  platform?: string | null,
  targetId?: string | null,
  targetType: AvatarTargetType = 'user'
): string | undefined {
  const avatarFetchEnabled = useAvatarFetchEnabled()
  const avatarPath = useMemo(
    () => buildWebuiAvatarPath(platform, targetId, targetType),
    [platform, targetId, targetType]
  )
  const [avatarUrl, setAvatarUrl] = useState<string | undefined>()

  useEffect(() => {
    let ignore = false
    if (!avatarFetchEnabled || !avatarPath) {
      setAvatarUrl(undefined)
      return
    }

    resolveApiPath(avatarPath).then((resolvedPath) => {
      if (!ignore) setAvatarUrl(resolvedPath)
    })

    return () => {
      ignore = true
    }
  }, [avatarFetchEnabled, avatarPath])

  return avatarUrl
}
