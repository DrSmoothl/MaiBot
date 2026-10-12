import { Link, useRouterState } from '@tanstack/react-router'
import {
  Activity,
  Eye,
  House,
  LayoutGrid,
  MessageCircle,
  Search,
  SlidersHorizontal,
  UserRound,
} from 'lucide-react'
import { useTranslation } from 'react-i18next'
import type { LucideIcon } from 'lucide-react'

import { cn } from '@/lib/utils'
import { resolveMobileTab } from './mobile-nav-context'
import type { MobileTab } from './mobile-nav-context'

interface MobileTabDef {
  tab: MobileTab
  to: string
  labelKey: string
  icon: LucideIcon
}

// 原版风格：四个标签 + 独立的搜索按钮；「我的」从管理页进入。
const GLASS_TABS: MobileTabDef[] = [
  { tab: 'home', to: '/', labelKey: 'mobileNav.home', icon: House },
  { tab: 'chat', to: '/chat', labelKey: 'mobileNav.chat', icon: MessageCircle },
  { tab: 'manage', to: '/manage', labelKey: 'mobileNav.manage', icon: LayoutGrid },
  { tab: 'observe', to: '/planner-monitor', labelKey: 'mobileNav.observe', icon: Activity },
]

// 千禧风格：一排五个键帽，搜索留在顶栏；图标沿用千禧图标集能顶替的图标名。
const KEYCAP_TABS: MobileTabDef[] = [
  { tab: 'home', to: '/', labelKey: 'mobileNav.home', icon: House },
  { tab: 'chat', to: '/chat', labelKey: 'mobileNav.chat', icon: MessageCircle },
  { tab: 'manage', to: '/manage', labelKey: 'mobileNav.manage', icon: SlidersHorizontal },
  { tab: 'observe', to: '/planner-monitor', labelKey: 'mobileNav.observe', icon: Eye },
  { tab: 'me', to: '/settings', labelKey: 'mobileNav.me', icon: UserRound },
]

// 液态玻璃材质：只用在悬浮层上，半透明磨砂 + 顶部细高光 + 轻投影。
export const MOBILE_GLASS_CLASS =
  'bg-background/70 backdrop-blur-2xl backdrop-saturate-150 shadow-[inset_0_0_0_0.5px_hsl(var(--foreground)/0.08),inset_0_1px_0_rgb(255_255_255/0.55),0_10px_30px_rgb(0_0_0/0.08),0_1px_2px_rgb(0_0_0/0.04)] dark:shadow-[inset_0_0_0_0.5px_rgb(255_255_255/0.12),inset_0_1px_0_rgb(255_255_255/0.08),0_10px_30px_rgb(0_0_0/0.4)]'

interface MobileTabBarProps {
  millennium: boolean
  onSearch: () => void
}

/** 移动端底部标签栏，取代侧栏与汉堡菜单；桌面端（lg 及以上）不渲染。 */
export function MobileTabBar({ millennium, onSearch }: MobileTabBarProps) {
  const { t } = useTranslation()
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const activeTab = resolveMobileTab(pathname)
  const tabs = millennium ? KEYCAP_TABS : GLASS_TABS

  return (
    <nav
      data-mobile-tabbar="true"
      aria-label={t('mobileNav.label')}
      className="fixed inset-x-4 bottom-[calc(env(safe-area-inset-bottom)+14px)] z-40 flex h-[62px] gap-2.5 lg:hidden"
    >
      <div
        data-mobile-tabbar-tabs="true"
        className={cn(
          'grid h-full flex-1 gap-0 rounded-full p-1',
          millennium ? 'grid-cols-5' : 'grid-cols-4',
          MOBILE_GLASS_CLASS
        )}
      >
        {tabs.map(({ tab, to, labelKey, icon: Icon }) => {
          const active = activeTab === tab
          return (
            <Link
              key={tab}
              to={to}
              data-mobile-tab="true"
              data-active={active ? 'true' : 'false'}
              aria-current={active ? 'page' : undefined}
              className={cn(
                'flex min-w-0 flex-col items-center justify-center gap-0.5 rounded-full text-[10px] font-semibold transition-colors',
                active ? 'bg-foreground/[0.06] text-primary' : 'text-foreground'
              )}
            >
              <Icon className="h-[22px] w-[22px]" strokeWidth={1.7} aria-hidden="true" />
              <span className="truncate">{t(labelKey)}</span>
            </Link>
          )
        })}
      </div>
      {!millennium && (
        <button
          type="button"
          data-mobile-tabbar-search="true"
          onClick={onSearch}
          aria-label={t('mobileNav.search')}
          className={cn(
            'text-foreground flex h-[62px] w-[62px] shrink-0 items-center justify-center rounded-full',
            MOBILE_GLASS_CLASS
          )}
        >
          <Search className="h-[22px] w-[22px]" strokeWidth={1.7} aria-hidden="true" />
        </button>
      )}
    </nav>
  )
}
