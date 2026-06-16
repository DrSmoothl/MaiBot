import { useEffect, useState } from 'react'
import { useNavigate } from '@tanstack/react-router'

import { checkAuthStatus } from '@/lib/auth'
import { authApi } from '@/lib/http'

const AUTH_STATUS_CACHE_MS = 30_000
let cachedAuthStatus: { authenticated: boolean; checkedAt: number } | null = null
let authStatusPromise: Promise<boolean> | null = null

function readCachedAuthStatus(): boolean | undefined {
  if (!cachedAuthStatus) {
    return undefined
  }
  if (Date.now() - cachedAuthStatus.checkedAt > AUTH_STATUS_CACHE_MS) {
    cachedAuthStatus = null
    return undefined
  }
  return cachedAuthStatus.authenticated
}

async function checkAuthStatusCached(): Promise<boolean> {
  const cached = readCachedAuthStatus()
  if (typeof cached === 'boolean') {
    return cached
  }
  authStatusPromise ??= checkAuthStatus().then((authenticated) => {
    cachedAuthStatus = { authenticated, checkedAt: Date.now() }
    return authenticated
  }).finally(() => {
    authStatusPromise = null
  })
  return authStatusPromise
}

export function useAuthGuard() {
  const navigate = useNavigate()
  const [checking, setChecking] = useState(readCachedAuthStatus() !== true)

  useEffect(() => {
    let cancelled = false
    const cached = readCachedAuthStatus()
    if (cached === true) {
      setChecking(false)
      return () => {
        cancelled = true
      }
    }
    
    const verifyAuth = async () => {
      try {
        const isAuth = await checkAuthStatusCached()
        if (!cancelled && !isAuth) {
          navigate({ to: '/auth' })
        }
      } catch {
        // 发生错误时也跳转到登录页
        if (!cancelled) {
          navigate({ to: '/auth' })
        }
      } finally {
        if (!cancelled) {
          setChecking(false)
        }
      }
    }
    
    verifyAuth()
    
    return () => {
      cancelled = true
    }
  }, [navigate])
  
  return { checking }
}

/**
 * 检查是否已认证（异步）
 */
export async function checkAuth(): Promise<boolean> {
  return await checkAuthStatus()
}

/**
 * 检查是否需要首次配置
 */
export async function checkFirstSetup(): Promise<boolean> {
  try {
    const data = await authApi.get<{ is_first_setup: boolean }>('/api/webui/setup/status')
    return data.is_first_setup
  } catch (error) {
    console.error('检查首次配置状态失败:', error)
    return false
  }
}
