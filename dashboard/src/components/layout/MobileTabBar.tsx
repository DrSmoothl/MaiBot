import { Link, useNavigate, useRouterState } from '@tanstack/react-router'
import {
  Activity,
  ChevronRight,
  Eye,
  House,
  LayoutGrid,
  MessageCircle,
  Search,
  SlidersHorizontal,
  UserRound,
} from 'lucide-react'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import type { LucideIcon } from 'lucide-react'

import { useMediaQuery } from '@/hooks/use-media-query'
import { cn } from '@/lib/utils'
import { StageStatusLabel } from '@/routes/monitor/StageStatusLabel'
import { useMaisakaMonitorOverview } from '@/routes/monitor/use-maisaka-monitor'
import type { StageStatusInfo } from '@/routes/monitor/use-maisaka-monitor'
import { MOBILE_GLASS_CLASS } from './mobile-glass'
import { resolveMobileTab } from './mobile-nav-context'
import type { MobileTab } from './mobile-nav-context'

interface MobileTabDef {
  tab: MobileTab
  to: string
  labelKey: string
  icon: LucideIcon
}

// 原版风格：四个标签 + 独立的搜索按钮；「我的」从首页头像进入。
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

// 与 Tailwind 的 lg 断点对齐：桌面端完全不挂载标签栏，也就不会订阅麦麦实时状态。
const MOBILE_MEDIA_QUERY = '(max-width: 1023.98px)'
// 页面向下滚动超过该距离后标签栏收成胶囊
const COLLAPSE_SCROLL_TOP = 48
const SCROLL_DELTA_THRESHOLD = 6

/** 底部浮层占用的高度写到根元素上，页面据此给内容留出底部空间 */
const DOCK_HEIGHT_CSS_VAR = '--mobile-dock-height'

interface MobileTabBarProps {
  millennium: boolean
  hidden?: boolean
  onSearch: () => void
}

/** 移动端底部标签栏，取代侧栏与汉堡菜单；桌面端（lg 及以上）不渲染。 */
export function MobileTabBar(props: MobileTabBarProps) {
  const isMobile = useMediaQuery(MOBILE_MEDIA_QUERY)

  useEffect(() => {
    if (isMobile && !props.hidden) return
    document.documentElement.style.setProperty(DOCK_HEIGHT_CSS_VAR, '0px')
  }, [isMobile, props.hidden])

  if (!isMobile || props.hidden) return null
  return props.millennium ? <KeycapTabBar /> : <GlassTabBar {...props} />
}

/** 记录底部浮层的实际高度，供页面留白使用 */
function useDockHeight(ref: React.RefObject<HTMLElement | null>) {
  useLayoutEffect(() => {
    const element = ref.current
    if (!element) return
    const update = () => {
      const bottomGap = window.innerHeight - element.getBoundingClientRect().bottom
      document.documentElement.style.setProperty(
        DOCK_HEIGHT_CSS_VAR,
        `${Math.ceil(element.offsetHeight + Math.max(0, bottomGap) + 16)}px`
      )
    }
    update()
    const observer = new ResizeObserver(update)
    observer.observe(element)
    return () => observer.disconnect()
  }, [ref])
}

/** 向下滚动时收起、向上滚动时展开；监听所有滚动容器，兼容页面内部自带滚动区域的情况 */
function useCollapseOnScroll(pathname: string) {
  // 记录在哪个页面上收起；换页后自然回到展开态
  const [collapsedPath, setCollapsedPath] = useState<string | null>(null)
  const pathnameRef = useRef(pathname)
  const lastScrollTops = useRef(new WeakMap<EventTarget, number>())

  useEffect(() => {
    pathnameRef.current = pathname
  }, [pathname])

  useEffect(() => {
    const handleScroll = (event: Event) => {
      const target = event.target
      if (!(target instanceof HTMLElement)) return
      // 横向滚动条、输入框等不参与判断
      if (target.scrollHeight - target.clientHeight < COLLAPSE_SCROLL_TOP * 2) return
      const scrollTop = target.scrollTop
      const previous = lastScrollTops.current.get(target) ?? scrollTop
      lastScrollTops.current.set(target, scrollTop)
      const delta = scrollTop - previous
      if (scrollTop <= COLLAPSE_SCROLL_TOP) {
        setCollapsedPath(null)
      } else if (delta > SCROLL_DELTA_THRESHOLD) {
        setCollapsedPath(pathnameRef.current)
      } else if (delta < -SCROLL_DELTA_THRESHOLD) {
        setCollapsedPath(null)
      }
    }
    document.addEventListener('scroll', handleScroll, { capture: true, passive: true })
    return () => document.removeEventListener('scroll', handleScroll, { capture: true })
  }, [])

  const collapsed = collapsedPath === pathname
  const setCollapsed = (value: boolean) => setCollapsedPath(value ? pathname : null)
  return [collapsed, setCollapsed] as const
}

/** 找出最近一次状态更新的聊天流，作为「麦麦实时」附件条的内容 */
function pickLatestStageStatus(stageStatuses: Map<string, StageStatusInfo>) {
  let latest: StageStatusInfo | undefined
  for (const status of stageStatuses.values()) {
    if (!latest || status.updatedAt > latest.updatedAt) latest = status
  }
  return latest
}

function GlassTabBar({ onSearch }: MobileTabBarProps) {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const activeTab = resolveMobileTab(pathname)
  const dockRef = useRef<HTMLDivElement>(null)
  const [collapsed, setCollapsed] = useCollapseOnScroll(pathname)
  const { sessions, stageStatuses, setSelectedSession } = useMaisakaMonitorOverview()
  const latestStatus = pickLatestStageStatus(stageStatuses)
  // 观察页本身就展示实时状态，不再重复显示附件条
  const showLive = latestStatus !== undefined && activeTab !== 'observe'
  const liveSessionName = latestStatus
    ? (sessions.get(latestStatus.sessionId)?.sessionName ??
      latestStatus.sessionName ??
      latestStatus.sessionId.slice(0, 8))
    : ''
  const activeDef = GLASS_TABS.find((def) => def.tab === activeTab) ?? GLASS_TABS[0]
  const ActiveIcon = activeDef.icon

  useDockHeight(dockRef)

  const openLive = () => {
    if (!latestStatus) return
    setSelectedSession(latestStatus.sessionId)
    void navigate({ to: '/planner-monitor' })
  }

  const liveStrip = latestStatus && (
    <button
      type="button"
      data-mobile-live-strip="true"
      onClick={openLive}
      aria-label={t('mobileNav.live')}
      className={cn(
        'text-foreground flex h-[52px] min-w-0 items-center gap-3 rounded-full pr-1.5 pl-2 text-left',
        collapsed ? 'flex-1' : 'w-full',
        MOBILE_GLASS_CLASS
      )}
    >
      <span className="bg-foreground text-background flex h-9 w-9 shrink-0 items-center justify-center rounded-full text-sm font-semibold">
        {t('mobileNav.botInitial')}
      </span>
      <span className="flex min-w-0 flex-1 flex-col">
        <span className="truncate text-sm font-semibold">
          {liveSessionName} · <StageStatusLabel status={latestStatus} />
        </span>
        {!collapsed && latestStatus.detail && (
          <span className="text-muted-foreground truncate text-xs">{latestStatus.detail}</span>
        )}
      </span>
      {!collapsed && (
        <span className="flex h-10 w-10 shrink-0 items-center justify-center">
          <ChevronRight className="h-[18px] w-[18px]" strokeWidth={1.7} aria-hidden="true" />
        </span>
      )}
    </button>
  )

  return (
    <div
      ref={dockRef}
      data-mobile-tabbar="true"
      data-collapsed={collapsed ? 'true' : 'false'}
      className="fixed inset-x-4 bottom-[calc(env(safe-area-inset-bottom)+14px)] z-40 flex flex-col gap-2.5"
    >
      {showLive && !collapsed && liveStrip}
      {collapsed ? (
        // 收起态：只保留当前标签（点按展开）与麦麦实时，iOS 26 的标签栏最小化形态
        <div className="flex items-center gap-2.5">
          <button
            type="button"
            data-mobile-tabbar-expand="true"
            aria-label={t(activeDef.labelKey)}
            onClick={() => setCollapsed(false)}
            className={cn(
              'text-primary flex h-[52px] w-[52px] shrink-0 items-center justify-center rounded-full',
              MOBILE_GLASS_CLASS
            )}
          >
            <ActiveIcon className="h-[22px] w-[22px]" strokeWidth={1.7} aria-hidden="true" />
          </button>
          {showLive ? liveStrip : <span className="flex-1" />}
          <button
            type="button"
            data-mobile-tabbar-search="true"
            onClick={onSearch}
            aria-label={t('mobileNav.search')}
            className={cn(
              'text-foreground flex h-[52px] w-[52px] shrink-0 items-center justify-center rounded-full',
              MOBILE_GLASS_CLASS
            )}
          >
            <Search className="h-[22px] w-[22px]" strokeWidth={1.7} aria-hidden="true" />
          </button>
        </div>
      ) : (
        <nav aria-label={t('mobileNav.label')} className="flex h-[62px] gap-2.5">
          <div
            data-mobile-tabbar-tabs="true"
            className={cn('grid h-full flex-1 grid-cols-4 rounded-full p-1', MOBILE_GLASS_CLASS)}
          >
            {GLASS_TABS.map(({ tab, to, labelKey, icon: Icon }) => {
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
        </nav>
      )}
    </div>
  )
}

function KeycapTabBar() {
  const { t } = useTranslation()
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const activeTab = resolveMobileTab(pathname)
  const dockRef = useRef<HTMLElement>(null)

  useDockHeight(dockRef)

  return (
    <nav
      ref={dockRef}
      data-mobile-tabbar="true"
      aria-label={t('mobileNav.label')}
      className="fixed inset-x-4 bottom-[calc(env(safe-area-inset-bottom)+14px)] z-40 flex h-[62px] gap-2.5"
    >
      <div data-mobile-tabbar-tabs="true" className="grid h-full flex-1 grid-cols-5 gap-0 p-1">
        {KEYCAP_TABS.map(({ tab, to, labelKey, icon: Icon }) => {
          const active = activeTab === tab
          return (
            <Link
              key={tab}
              to={to}
              data-mobile-tab="true"
              data-active={active ? 'true' : 'false'}
              aria-current={active ? 'page' : undefined}
              className="flex min-w-0 flex-col items-center justify-center gap-0.5 text-[10px] font-semibold"
            >
              <Icon className="h-[22px] w-[22px]" strokeWidth={1.7} aria-hidden="true" />
              <span className="truncate">{t(labelKey)}</span>
            </Link>
          )
        })}
      </div>
    </nav>
  )
}
