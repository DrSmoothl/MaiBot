import { Link, useRouterState } from '@tanstack/react-router'
import { useContext } from 'react'
import { useTranslation } from 'react-i18next'

import { ThemeProviderContext } from '@/lib/theme-context'
import { cn } from '@/lib/utils'
import { MobilePageHeader } from './MobilePageHeader'
import { resolveMobileTab } from './mobile-nav-context'
import type { MenuSection } from './types'

/** 这些页面自己画手机页头（首页、聊天、管理），布局不再重复 */
const SELF_HEADED_PATHS = new Set(['/', '/chat', '/manage'])

/** 「观察」标签下的三个视图，用分段控件切换 */
const OBSERVE_SEGMENTS = [
  { to: '/planner-monitor', labelKey: 'mobileNav.observeLive' },
  { to: '/logs', labelKey: 'mobileNav.observeLogs' },
  { to: '/reasoning-process', labelKey: 'mobileNav.observeReasoning' },
] as const

/** 在菜单里找当前路径对应的入口名称：取最长的路径前缀匹配 */
function findMenuLabel(
  menuSections: MenuSection[],
  pathname: string,
  translate: (key: string) => string
): string | null {
  let best: { length: number; label: string } | null = null
  for (const section of menuSections) {
    for (const item of section.items) {
      if (item.external) continue
      const matches = pathname === item.path || pathname.startsWith(`${item.path}/`)
      if (matches && item.path !== '/' && (!best || item.path.length > best.length)) {
        best = {
          length: item.path.length,
          label: item.literalLabel ? item.label : translate(item.label),
        }
      }
    }
  }
  return best?.label ?? null
}

interface MobileRouteHeaderProps {
  menuSections: MenuSection[]
}

/**
 * 手机端各子页面统一的页头：大标题取自菜单名称，管理类页面左上角返回「管理」，
 * WebUI 设置返回首页（从首页头像进入），观察类页面下方是视图切换。
 */
export function MobileRouteHeader({ menuSections }: MobileRouteHeaderProps) {
  const { t } = useTranslation()
  const { themeConfig } = useContext(ThemeProviderContext)
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  if (SELF_HEADED_PATHS.has(pathname) || pathname.startsWith('/extensions/')) return null

  const tab = resolveMobileTab(pathname)

  if (tab === 'observe') {
    return (
      <MobilePageHeader title={t('mobileNav.observe')} className="px-4 pt-3 pb-2">
        <nav
          aria-label={t('mobileNav.observe')}
          data-mobile-segmented="true"
          className="bg-muted grid grid-cols-3 rounded-[10px] p-0.5"
        >
          {OBSERVE_SEGMENTS.map((segment) => {
            const active = pathname.startsWith(segment.to)
            return (
              <Link
                key={segment.to}
                to={segment.to}
                aria-current={active ? 'page' : undefined}
                data-active={active ? 'true' : 'false'}
                className={cn(
                  'flex h-[30px] items-center justify-center rounded-lg text-sm',
                  active && 'bg-background font-semibold shadow-[0_1px_3px_rgb(0_0_0/0.12)]'
                )}
              >
                {t(segment.labelKey)}
              </Link>
            )
          })}
        </nav>
      </MobilePageHeader>
    )
  }

  if (tab === 'me') {
    // 原版风格从首页头像进入，需要返回；千禧风格的「我的」本身就是一个标签
    return (
      <MobilePageHeader
        title={t('sidebar.menu.settings')}
        back={
          themeConfig.dashboardStyle === 'millennium'
            ? undefined
            : { to: '/', label: t('mobileNav.backHome') }
        }
        className="px-4 pt-3 pb-2"
      />
    )
  }

  const title = findMenuLabel(menuSections, pathname, t)
  if (!title) return null
  return (
    <MobilePageHeader
      title={title}
      back={{ to: '/manage', label: t('mobileNav.backManage') }}
      className="px-4 pt-3 pb-2"
    />
  )
}
